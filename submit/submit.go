package submit

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/base2genomics/batchit"

	arg "github.com/alexflint/go-arg"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ecs"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/brentp/xopen"
	"github.com/pkg/errors"
)

type cliargs struct {
	Image     string   `arg:"-i,required,help:image like $acct.dkr.ecr.$region.amazonaws.com/$image:$tag or $image:$tag"`
	Registry  string   `arg:"env" help:"Docker image registry. [default: $acct.dkr.ecr.$region.amazonaws.com]"`
	Role      string   `arg:"-r,required,help:existing role name"`
	Region    string   `arg:"env:AWS_DEFAULT_REGION,help:region for batch setup"`
	Queue     string   `arg:"-q,required,help:job queue"`
	ArraySize int64    `arg:"-a,help:optional size of array job"`
	DependsOn []string `arg:"-d,help:jobId(s) that this job depends on"`
	Retries   int64    `arg:"-r,help:number of times to retry this job on failure"`
	EnvVars   []string `arg:"-v,help:key-value environment pairs of the form NAME=value"`
	CPUs      int      `arg:"-c,help:number of cpus reserved by the job"`
	Volumes   []string `arg:"-o,help:HOST_PATH=CONTAINER_PATH"`
	S3Outputs string   `arg:"help:comma-delimited list of s3 paths indicating the output of this run. If all present job will *not* be run."`
	Mem       int      `arg:"-m,help:memory (MiB) reserved by the job"`
	Ebs       string   `arg:"-e,help:args for ebs mount. format mount-point:size:volume-type:fstype eg /mnt/xx:500:sc1:ext4 where last 2 arguments are optional and default as shown. This assumes that batchit is installed on the host. If type==io1 the 5th argument must specify the IOPs (between 100 and 20000)"`
	JobName   string   `arg:"-j,required,help:name of job"`
	Path      string   `arg:"required,positional,help:path of bash script to run. With '-' it will be read from STDIN. Prefix with 'script:' to send a string."`
}

func (c cliargs) Version() string {
	return batchit.Version
}

func getRole(svc *iam.IAM, role string) *iam.Role {
	inp := &iam.GetRoleInput{RoleName: &role}
	op, err := svc.GetRole(inp)
	if err != nil {
		panic(err)
	}
	return op.Role
}

const scriptPrefix = "script:"
const interactivePrefix = "interactive:"

// gzip and then base64 encode a shell script.
func shellEncode(path string) string {
	var b bytes.Buffer
	enc := base64.NewEncoder(base64.StdEncoding, &b)
	z := gzip.NewWriter(enc)
	if strings.HasPrefix(path, scriptPrefix) {
		if _, err := z.Write([]byte(path[len(scriptPrefix):])); err != nil {
			panic(err)
		}
	} else if strings.HasPrefix(path, interactivePrefix) {
		tmp := strings.Split(path, ":")
		minutes := 20
		if len(tmp) == 2 {
			m, err := strconv.Atoi(tmp[1])
			if err == nil {
				minutes = m
			} else {
				log.Println("couldn't parse minutes from %s", tmp[1])
			}
		}
		if _, err := z.Write([]byte(fmt.Sprintf("sleep %d", minutes*60))); err != nil {
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
trap "cleanup_volume EXIT; cleanup;" EXIT
mkdir -p ${TMPDIR}/tmp/
mount --bind ${TMPDIR}/tmp/ /tmp/
cd $TMPDIR`, mnt)
	return tmp
}

var NotFound = errors.New("not found")

// return that the file exists, its size, and any error
func OutputExists(s3o *s3.S3, path string) (bool, int64, error) {
	if strings.HasPrefix(path, "s3://") {
		path = path[5:]
	}
	bk := strings.SplitN(path, "/", 2)
	ho, err := s3o.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(bk[0]), Key: aws.String(bk[1])})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "Forbidden":
				return false, 0, fmt.Errorf("you do not have permissions to access %s", path)
			case "NotFound":
				return false, 0, NotFound
			default:
				return false, 0, aerr
			}

		}
		return false, 0, err
	}

	return ho.ContentLength != nil && *ho.ContentLength > 0, *ho.ContentLength, nil
}

func outputsExist(sess *session.Session, paths []string) bool {
	svc := s3.New(sess)
	for _, p := range paths {
		found, _, err := OutputExists(svc, p)
		if err != nil && err != NotFound {
			log.Fatal(err)
		}
		if !found {
			return false
		}
	}
	return true
}

func Main() {
	cli := &cliargs{CPUs: 1, Mem: 1048, Retries: 1, Region: "us-east-1"}
	p := arg.MustParse(cli)

	cfg := aws.NewConfig().WithRegion(cli.Region)
	sess := session.Must(session.NewSession(cfg))

	if cli.S3Outputs != "" {
		if outputsExist(sess, strings.Split(cli.S3Outputs, ",")) {
			max := 100
			if max > len(cli.S3Outputs) {
				max = len(cli.S3Outputs)
			}
			fmt.Fprintln(os.Stderr, "[batchit submit] all output found for "+cli.S3Outputs[0:max]+"... not re-running\n")
			return
		}
	}
	cleanupDefault := `cleanup_volume() { echo "batchit: No volumes to clean up"; }`
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
		ebsCmd[2] = fmt.Sprintf(`cleanup_volume() { set +e; sig="$1"; echo "batchit: cleaning up volume $vid on signal $sig"; umount %s || umount -l %s; batchit ddv $vid; if [[ $sig != EXIT ]]; then trap - $sig EXIT; kill -s $sig $$; fi }; for sig in INT TERM EXIT; do trap "cleanup_volume $sig" $sig; done`, ebs[0], ebs[0])
	}

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
if [ -f /etc/default/locale ]; then source /etc/default/locale; fi
set +a
if [ -f /etc/profile ]; then source /etc/profile; fi
set -Eeuo pipefail
%s
%s
%s
%s
%s
export BATCH_SCRIPT=$(mktemp)
echo "$B64GZ" | base64 -d | gzip -dc > $BATCH_SCRIPT
chmod +x $BATCH_SCRIPT
$BATCH_SCRIPT
			`, cleanupDefault, ebsCmd[0], ebsCmd[1], ebsCmd[2], tmpMnt)), "\n") {
		tmp := strings.TrimSpace(line[:])
		if len(tmp) != 0 {
			commands = append(commands, &tmp)
		}
	}

	if cli.S3Outputs != "" {
		cmd := fmt.Sprintf("batchit s3upload -c --region %s --nofail %s", cli.Region, strings.Join(strings.Split(cli.S3Outputs, ","), " "))
		commands = append(commands, &cmd)
	}

	if cli.Registry == "" {
		if !strings.Contains(cli.Image, "/") {
			stsvc := sts.New(sess)
			user, err := stsvc.GetCallerIdentity(&sts.GetCallerIdentityInput{})
			if err != nil {
				panic(err)
			}
			cli.Image = fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com/%s", *user.Account, *sess.Config.Region, cli.Image)
		}
	} else {
		if cli.Registry == "hub.docker.com" || cli.Registry == "docker.com" {
			cli.Registry = "registry.hub.docker.com"
		}
		if cli.Registry == "registry.hub.docker.com" {
			if !strings.Contains(cli.Image, "/") {
				cli.Image = fmt.Sprintf("library/%s", cli.Image)
			}
		}
		cli.Image = fmt.Sprintf("%s/%s", cli.Registry, cli.Image)
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
	// Ignore return value; there's not much we can do if it fails
	// (and we're no worse off than before.)
	defer deleteJobDefinition(b, ro)
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

	if strings.HasPrefix(cli.Path, interactivePrefix) {
		showConnectionInfo(b, *resp.JobId, sess, cli.Queue)
	}
	fmt.Println(*resp.JobId)
}

func getCluster(b *batch.Batch, q string, keyPair *string) string {

	qi := &batch.DescribeJobQueuesInput{JobQueues: []*string{&q}}
	qr, err := b.DescribeJobQueues(qi)
	if err != nil {
		log.Println(err)
		os.Exit(0)
	}
	if len(qr.JobQueues) > 1 {
		log.Println("instance info only supported for queues with a single compute env")
	}
	ce := qr.JobQueues[0].ComputeEnvironmentOrder[0].ComputeEnvironment

	ci := &batch.DescribeComputeEnvironmentsInput{
		ComputeEnvironments: []*string{ce},
	}
	cr, err := b.DescribeComputeEnvironments(ci)
	if err != nil {
		log.Println(err)
		os.Exit(0)
	}
	*keyPair = *cr.ComputeEnvironments[0].ComputeResources.Ec2KeyPair
	return *cr.ComputeEnvironments[0].EcsClusterArn
}

func showConnectionInfo(b *batch.Batch, jobid string, sess *session.Session, queue string) {
	log.Println("waiting for job to start to get connection info")

	dji := &batch.DescribeJobsInput{
		Jobs: []*string{&jobid},
	}
	for i := 0; i < 100; i++ {
		time.Sleep(20 * time.Second)
		djo, err := b.DescribeJobs(dji)
		if err != nil {
			log.Println(err)
			os.Exit(0)
		}
		if djo == nil {
			break
		}
		var j = djo.Jobs[0]
		if *j.Status != "RUNNING" {
			log.Println("job status is ", *j.Status, " waiting")
			continue
		}

		var ec = ecs.New(sess)
		var keyPair = ""
		var cluster = getCluster(b, queue, &keyPair)

		tmp := strings.Split(*j.Container.ContainerInstanceArn, "/")
		ei := &ecs.DescribeContainerInstancesInput{
			Cluster:            aws.String(cluster),
			ContainerInstances: []*string{&tmp[1]},
		}

		eo, err := ec.DescribeContainerInstances(ei)
		if err != nil {
			log.Fatal(err)
		}

		instanceId := *eo.ContainerInstances[0].Ec2InstanceId
		ec2s := ec2.New(sess)
		log.Println("instance-id:", instanceId)

		di := &ec2.DescribeInstancesInput{InstanceIds: []*string{&instanceId}}

		do, err := ec2s.DescribeInstances(di)
		if err != nil {
			log.Fatal(err)
		}

		ti := &ecs.DescribeTasksInput{Cluster: aws.String(cluster), Tasks: []*string{j.Container.TaskArn}}
		to, err := ec.DescribeTasks(ti)
		if err != nil {
			log.Fatal(err)
		}

		if len(to.Tasks) != 1 {
			log.Println("couldn't find container id")
		}

		c := to.Tasks[0].Containers[0]
		_ = c
		//log.Println(to)
		//log.Println(j.Container)

		dockerCmd := fmt.Sprintf(`docker exec -it $(curl -s "http://127.0.0.1:51678/v1/tasks?taskarn=%s" | grep -oP "DockerId..\"[^\"]+" | cut -d\" -f 3) bash`, *j.Container.TaskArn)

		log.Printf("ssh -ti ~/.ssh/%s.pem ec2-user@%s '%s'", keyPair, *do.Reservations[0].Instances[0].PublicIpAddress, dockerCmd)
		//log.Println("TODO: get container from Task:", *j.Container.TaskArn, " https://docs.aws.amazon.com/sdk-for-go/api/service/ecs/#Task")
		// ssh -ti ~/.ssh/istore.pem ec2-user@34.203.245.158 'docker exec -it $(curl -s "http://127.0.0.1:51678/v1/tasks?taskarn=arn:aws:ecs:us-east-1:321620740768:task/c8fcafec-2f0b-4129-8b21-7fae81ae8be9" | grep -oP "DockerId..\"[^\"]+" | cut -d\" -f 3) bash'
		break
		/*

			di := &ec2.DescribeAddressesInput{
				//Filters: []*ec2.Filter{
				//	&ec2.Filter{Name: aws.String("instance-id"), Values: []*string{&instanceId}}},
				Filters: []*ec2.Filter{
					{
						Name:   aws.String("domain"),
						Values: aws.StringSlice([]string{"vpc"}),
					},
				},
			}
			do, err := ec2s.DescribeAddresses(di)
			if err != nil {
				log.Fatal(err)
			}
			log.Println(do)
			log.Println(*do.Addresses[0].PublicIp)
		*/

	}

}

func deleteJobDefinition(b *batch.Batch, jdef *batch.RegisterJobDefinitionOutput) error {
	jobDefToDelete := fmt.Sprintf("%s:%d", *jdef.JobDefinitionName, *jdef.Revision)
	input := &batch.DeregisterJobDefinitionInput{
		JobDefinition: aws.String(jobDefToDelete),
	}
	_, err := b.DeregisterJobDefinition(input)
	return err
}
