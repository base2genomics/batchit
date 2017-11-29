package ddv

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/base2genomics/batchit/exsmount"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

func DetachAndDelete(vid string) error {
	var svc *ec2.EC2
	var drsp *ec2.DescribeVolumesOutput
	var err error

	for _, region := range []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2", "ap-south-1",
		"ap-northeast-2",
		"ap-northeast-1",
		"ca-central-1",
		"cn-north-1",
		"eu-west-1",
		"eu-west-2",
		"sa-east-1",
		"us-gov-west-1",
		"ap-southeast-1",
		"ap-southeast-2",
	} {
		svc = ec2.New(session.Must(session.NewSession()), &aws.Config{Region: &region})
		drsp, err = svc.DescribeVolumes(
			&ec2.DescribeVolumesInput{
				VolumeIds: []*string{&vid},
			})
		if err != nil {
			continue
		}
		break
	}
	if drsp == nil {
		return fmt.Errorf("ddv: volume: %s not found", vid)
	}
	if err != nil {
		return err
	}

	log.Printf("ddv: found volume for deletion in region: %s", *svc.Config.Region)

	dtvi := &ec2.DetachVolumeInput{
		VolumeId: aws.String(vid),
		Force:    aws.Bool(true),
	}

	var v *ec2.VolumeAttachment

	for i := 0; i < 10; i++ {
		v, err = svc.DetachVolume(dtvi)
		if err == nil {
			if err := exsmount.WaitForVolumeStatus(svc, &vid, "available"); err != nil {
				return err
			}
			break
		}
		if strings.Contains(err.Error(), "is in the 'available' state") {
			break
		}
		if v != nil && *v.State == "available" {
			break
		}
		if err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
	}

	if _, err := svc.DeleteVolume(&ec2.DeleteVolumeInput{VolumeId: aws.String(vid)}); err != nil {
		return err
	}
	return nil
}

func Main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: ddv [<volume-id> ... ]")
		os.Exit(1)
	}
	wg := &sync.WaitGroup{}
	for _, vid := range os.Args[1:] {
		wg.Add(1)
		go func(vid string) {

			if err := DetachAndDelete(vid); err != nil {
				log.Println(err)
			} else {
				log.Printf("volume %s has been deleted", vid)
			}
			wg.Done()
		}(vid)
	}
	wg.Wait()
	// always has non-zero exit status.
}
