package submit

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/base2genomics/batchit"

	arg "github.com/alexflint/go-arg"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/brentp/xopen"
	"github.com/pkg/errors"
)

type cliargs struct {
	Image     string   `arg:"-i,required,help:ECR image like $acct.dkr.ecr.$region.amazonaws.com/$tag"`
	Role      string   `arg:"-r,required,help:existing role name"`
	Region    string   `arg:"help:region for batch setup"`
	Queue     string   `arg:"-q,required,help:job queue"`
	ArraySize int64    `arg:"-a,help:optional size of array job"`
	DependsOn []string `arg:"-d,help:jobId(s) that this job depends on"`
	Retries   int64    `arg:"-r,help:number of times to retry this job on failure"`
	EnvVars   []string `arg:"-v,help:key-value environment pairs of the form NAME=value"`
	CPUs      int      `arg:"-c,help:number of cpus reserved by the job"`
	Volumes   []string `arg:"-o,help:HOST_PATH=CONTAINER_PATH"`
	Mem       int      `arg:"-m,help:memory (MiB) reserved by the job"`
	Ebs       string   `arg:"-e,help:args for ebs mount. format mount-point:size:volume-type:fstype eg /mnt/xx:500:sc1:ext4 where last 2 arguments are optional and default as shown. This assumes that batchit is installed on the host. If type==io1 the 5th argument must specify the IOPs (between 100 and 20000)"`
	JobName   string   `arg:"-j,required,help:name of job"`
	Path      string   `arg:"required,positional,help:path of bash script to run. With '-' it will be read from STDIN. Prefix with 'script:' to send a string."`
}

func (c cliargs) Version() string {
	return batchit.Version
}

func getRole(svc *iam.IAM, role string) *iam.Role {

	var roles []*iam.Role
	var marker *string

	for {

		params := &iam.ListRolesInput{
			MaxItems: aws.Int64(100),
			Marker:   marker,
		}

		r, err := svc.ListRoles(params)
		if err != nil {
			panic(err)
		}
		roles = append(roles, r.Roles...)
		if !*r.IsTruncated {
			break
		}
		marker = r.Marker
	}
	var irole *iam.Role
	for _, r := range roles {
		if *r.RoleName == role {
			irole = r
			break
		}
	}

	return irole
}

const scriptPrefix = "script:"

// gzip and then base64 encode a shell script.
func shellEncode(path string) string {
	var b bytes.Buffer
	enc := base64.NewEncoder(base64.StdEncoding, &b)
	z := gzip.NewWriter(enc)
	if strings.HasPrefix(path, scriptPrefix) {
		if _, err := z.Write([]byte(path[len(scriptPrefix):])); err != nil {
			panic(err)
		}

	} else {
		rdr, err := xopen.Ropen(path)
		if err != nil {
			panic(err)
		}
		_, err = io.Copy(z, rdr)
		if err != nil {
			panic(err)
		}
	}
	if err := z.Close(); err != nil {
		panic(err)
	}
	if err := enc.Close(); err != nil {
		panic(err)
	}
	return b.String()
}

func getTmp(cli *cliargs) string {
	if len(cli.Volumes) == 0 {
		return ""
	}
	mnt := strings.Split(cli.Volumes[0], "=")[1]
	tmp := fmt.Sprintf(`# thanks Hao
export TMPDIR="$(mktemp -d -p %s)"
cleanup() { echo "batchit: deleting temp dir ${TMPDIR}"; umount -l /tmp/; rm -rf ${TMPDIR}; }
trap cleanup EXIT
mkdir -p ${TMPDIR}/tmp/
mount --bind ${TMPDIR}/tmp/ /tmp/
cd $TMPDIR`, mnt)
	return tmp
}

func Main() {
	cli := &cliargs{CPUs: 1, Mem: 1048, Retries: 1, Region: "us-east-1"}
	p := arg.MustParse(cli)
	var ebsCmd [3]string
	if len(cli.Ebs) > 0 {
		ebs := strings.Split(cli.Ebs, ":")
		if len(ebs) == 3 {
			ebs = append(ebs, "ext4")
		}
		if len(ebs) == 2 {
			_, err := strconv.Atoi(ebs[1])
			if err != nil {
				panic(fmt.Sprintf("error with specified ebs drive size: %s, %s", ebs[1], err))
			}
			ebs = append(ebs, []string{"gp2", "ext4"}...)
		}
		if len(ebs) != 4 && len(ebs) != 5 {
			p.Fail(fmt.Sprintf("expected Ebs argument to have 2 or 4 arguments"))
		}
		sz, err := strconv.Atoi(ebs[1])
		if err != nil {
			panic(fmt.Sprintf("error with specified ebs drive size: %s, %s", ebs[1], err))
		}
		//Ebs   /mnt/local:500:gp2:ext4
		// if possible, we raid-0 2 or 3 drives for better performance.
		// http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html
		// gp2/st1 bandwith maxes at 3,334 GB/ 12.5TB so we RAID0 after that.
		n := 1
		if (ebs[2] == "gp2" && sz > 3400) || (ebs[2] == "st1" && sz >= 12500) {
			n = 2
		}
		if len(ebs) == 4 {
			ebsCmd[0] = fmt.Sprintf("export vid=$(batchit ebsmount -n %d -m %s -s %s -v %s -t %s)", n, ebs[0], ebs[1], ebs[2], ebs[3])
		} else {
			ebsCmd[0] = fmt.Sprintf("export vid=$(batchit ebsmount -n %d -m %s -s %s -v %s -t %s -i %s)", n, ebs[0], ebs[1], ebs[2], ebs[3], ebs[4])
		}
		// mount the ebs volume and set trap to delete and detach the volume upon exit.
		ebsCmd[1] = `echo "vid: $vid"`
		// volumes get deleted at instance termination, but this will delete when the container exits.
		// unsets the trap for exit if it was already set to avoid loop.
		ebsCmd[2] = fmt.Sprintf(`for sig in INT TERM EXIT; do trap "set +e; umount %s || umount -l %s; batchit ddv $vid; if [[ $sig != EXIT ]]; then trap - $sig EXIT; kill -s $sig $$; fi" $sig; done`, ebs[0], ebs[0])
	}

	cfg := aws.NewConfig().WithRegion(cli.Region)
	sess := session.Must(session.NewSession(cfg))
	role := getRole(iam.New(sess, cfg), cli.Role)
	if role == nil {
		panic(fmt.Sprintf("role: %s not found for your account in region: %s", cli.Role, cli.Region))
	}
	b := batch.New(sess, cfg)
	tmpMnt := getTmp(cli)

	payload := shellEncode(cli.Path)
	var commands []*string
	// prelude copied from aegea.
	for _, line := range strings.Split(strings.TrimSpace(fmt.Sprintf(`
/bin/bash
-c
for i in "$@"; do eval "$i"; done
batchit
set -a
if [ -f /etc/environment ]; then source /etc/environment; fi
if [ -f /etc/default/locale ]; then source /etc/default/locale; fi
set +a
if [ -f /etc/profile ]; then source /etc/profile; fi
set -euo pipefail
%s
%s
%s
%s
export BATCH_SCRIPT=$(mktemp)
echo "$B64GZ" | base64 -d | gzip -dc > $BATCH_SCRIPT
chmod +x $BATCH_SCRIPT
$BATCH_SCRIPT
			`, ebsCmd[0], ebsCmd[1], ebsCmd[2], tmpMnt)), "\n") {
		tmp := strings.TrimSpace(line[:])
		if len(tmp) != 0 {
			commands = append(commands, &tmp)
		}
	}

	if !strings.Contains(cli.Image, "/") {
		stsvc := sts.New(sess)
		user, err := stsvc.GetCallerIdentity(&sts.GetCallerIdentityInput{})
		if err != nil {
			panic(err)
		}
		cli.Image = fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com/%s", *user.Account, *sess.Config.Region, cli.Image)
	}
	var arrayProp *batch.ArrayProperties
	if cli.ArraySize != 0 {
		arrayProp = &batch.ArrayProperties{Size: aws.Int64(cli.ArraySize)}
	}

	jdef := &batch.RegisterJobDefinitionInput{
		JobDefinitionName: &cli.JobName,
		RetryStrategy:     &batch.RetryStrategy{Attempts: aws.Int64(cli.Retries)},
		ContainerProperties: &batch.ContainerProperties{Image: &cli.Image, JobRoleArn: role.Arn,
			Memory:  aws.Int64(int64(cli.Mem)),
			Command: commands,
			Ulimits: []*batch.Ulimit{&batch.Ulimit{HardLimit: aws.Int64(40000), SoftLimit: aws.Int64(40000), Name: aws.String("nofile")}},
			Environment: []*batch.KeyValuePair{&batch.KeyValuePair{Name: aws.String("B64GZ"),
				Value: aws.String(payload)}},
			Privileged: aws.Bool(true),
			Vcpus:      aws.Int64(int64(cli.CPUs))},
		Type: aws.String("container"),
	}
	if cli.Ebs != "" {
		// see: http://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_data_volumes.html
		// without cloud-init, we must mount /dev by name.This means that the the EBS vol won't get
		// cleaned up by default.
		jdef.ContainerProperties.Volumes = []*batch.Volume{
			&batch.Volume{Name: aws.String("vol00"), Host: &batch.Host{SourcePath: aws.String("/dev")}},
		}
		jdef.ContainerProperties.MountPoints = []*batch.MountPoint{&batch.MountPoint{
			SourceVolume:  aws.String("vol00"),
			ContainerPath: aws.String("/dev"),
		}}
	}
	if len(cli.Volumes) > 0 {
		for k, v := range cli.Volumes {
			split := strings.Split(v, "=")
			if len(split) != 2 {
				panic("expected Volumes in the form: HOST_PATH=CONTAINER_PATH")
			}
			name := fmt.Sprintf("volxx%d", k)
			jdef.ContainerProperties.Volumes = append(jdef.ContainerProperties.Volumes,
				&batch.Volume{Host: &batch.Host{SourcePath: aws.String(split[0])}, Name: aws.String(name)})
			jdef.ContainerProperties.MountPoints = append(jdef.ContainerProperties.MountPoints,
				&batch.MountPoint{SourceVolume: aws.String(name), ContainerPath: aws.String(split[1])})
		}
	}

	ro, err := b.RegisterJobDefinition(jdef)
	if err != nil {
		panic(errors.Wrap(err, "error registering job definition"))
	}
	var deps []*batch.JobDependency
	for _, dep := range cli.DependsOn {
		deps = append(deps, &batch.JobDependency{JobId: aws.String(dep)})
	}

	submit := &batch.SubmitJobInput{
		DependsOn:       deps,
		JobDefinition:   ro.JobDefinitionName,
		JobName:         aws.String(cli.JobName),
		ArrayProperties: arrayProp,
		JobQueue:        aws.String(cli.Queue),
		ContainerOverrides: &batch.ContainerOverrides{
			Command: commands,
			Environment: []*batch.KeyValuePair{
				&batch.KeyValuePair{Name: aws.String("B64GZ"),
					Value: aws.String(payload)},
				&batch.KeyValuePair{Name: aws.String("cpus"),
					Value: aws.String(strconv.Itoa(cli.CPUs))},
			},
		},
	}
	if cli.Ebs != "" {
		// set TMPDIR to the EBS mount.
		ebs := strings.Split(cli.Ebs, ":")
		submit.ContainerOverrides.Environment = append(submit.ContainerOverrides.Environment,
			&batch.KeyValuePair{Name: aws.String("TMPDIR"), Value: aws.String(ebs[0])})
	}

	for _, e := range cli.EnvVars {
		pair := strings.SplitN(e, "=", 2)
		if len(pair) != 2 {
			panic(fmt.Sprintf("expecting EnvVars of format key=value. got %s", e))
		}
		submit.ContainerOverrides.Environment = append(submit.ContainerOverrides.Environment,
			&batch.KeyValuePair{Name: aws.String(pair[0]), Value: aws.String(pair[1])})
	}

	resp, err := b.SubmitJob(submit)
	if err != nil {
		if resp != nil {
			fmt.Fprintln(os.Stderr, resp)
		}
		panic(errors.Wrap(err, "error submitting job"))
	}
	fmt.Println(*resp.JobId)
}
