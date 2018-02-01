package logof

import (
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

func LogOf(jobId string, region string) int {
	input := batch.DescribeJobsInput{Jobs: []*string{aws.String(jobId)}}
	cfg := aws.NewConfig().WithRegion(region)
	sess := session.Must(session.NewSession(cfg))
	b := batch.New(sess, cfg)
	output, err := b.DescribeJobs(&input)
	if err != nil {
		log.Printf("[batchit] error finding jobs: %s in %s", jobId, region)
		log.Println(err)
		os.Exit(1)
	}
	if len(output.Jobs) == 0 {
		return 0
	}
	sort.Slice(output.Jobs, func(i, j int) bool { return *output.Jobs[i].StartedAt < *output.Jobs[j].StartedAt })
	j := output.Jobs[len(output.Jobs)-1]
	stream := j.Container.LogStreamName

	gli := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String("/aws/batch/job"),
		LogStreamName: stream,
		StartFromHead: aws.Bool(true),
	}

	cloud := cloudwatchlogs.New(sess, cfg)

	for {
		ev, err := cloud.GetLogEvents(gli)
		if err != nil {
			panic(err)
		}
		for _, event := range ev.Events {
			t := time.Unix(*event.Timestamp/1000, 0)
			fmt.Println("[" + t.Format(time.ANSIC) + "] " + *event.Message)
		}
		if ev.NextForwardToken == nil || (gli.NextToken != nil && *ev.NextForwardToken == *gli.NextToken) {
			break
		}
		gli.NextToken = ev.NextForwardToken
	}
	return 0
}

func Main() {
	if len(os.Args) < 3 {
		fmt.Println("usage: batchit logof JobId region")
		os.Exit(1)
	}
	os.Exit(LogOf(os.Args[1], os.Args[2]))
}
