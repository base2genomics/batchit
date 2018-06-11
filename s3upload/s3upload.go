package s3upload

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/base2genomics/batchit/submit"

	arg "github.com/alexflint/go-arg"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type cliargs struct {
	Region    string   `arg:"env:AWS_DEFAULT_REGION,help:region for batch setup"`
	Check     bool     `arg:"-c,help:check if file exists before uploading and don't upload if it is same size."`
	NoFail    bool     `arg:"help:don't fail if one of the local paths corresponding to an S3 path is not found."`
	Processes int      `arg:"-p,help:number of parallel uploads."`
	S3Paths   []string `arg:"required,positional,help:S3 destination paths. The final entry in the Key will be used to look for the local file."`
}

func (c cliargs) Description() string {
	return `Upload files to S3 in parallel using convention (file-naming)
This program requires that if you want to upload to s3://bucket/where/to/send.txt
a local file named 'send.txt' will exist. This program will upload the first 'send.txt' it finds.

To upload only files that are not already present, use '-c'. To not fail even if a local file is not found, use --nofail.
With '-c', if the local size does not match the size in S3, the file will be uploaded.
	`
}

func findIn(haystack []string, needle string) int {
	for i, h := range haystack {
		if needle == h {
			return i
		}
	}
	return -1
}

func getupload(s3paths []string, svc *s3.S3, check bool, nofail bool) ([]*s3manager.UploadInput, error) {
	uploads := make([]*s3manager.UploadInput, 0, len(s3paths))
	localpaths := make([]string, len(s3paths))
	founds := make([]bool, len(s3paths))

	for i, s3path := range s3paths {
		if strings.HasPrefix(s3path, "s3://") {
			s3path = s3path[5:]
		}

		tmp := strings.Split(s3path, "/")
		localpaths[i] = tmp[len(tmp)-1]
	}

	err := filepath.Walk(".", func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		tmp := strings.Split(f.Name(), "/")

		idx := findIn(localpaths, tmp[len(tmp)-1])
		if idx == -1 {
			return nil
		}
		founds[idx] = true
		s3path := s3paths[idx]
		if check {
			// check if file exists in s3
			exists, size, err := submit.OutputExists(svc, s3path)
			if err != nil && err != submit.NotFound {
				return err
			}
			if err == nil && exists && size == f.Size() {
				fmt.Fprintf(os.Stderr, "[batchit s3uploader] %s already in s3, skipping\n", f.Name())
				return nil
			}

		}

		fp, err := os.Open(f.Name())
		if err != nil {
			return err
		}
		if strings.HasPrefix(s3path, "s3://") {
			s3path = s3path[5:]
		}
		bk := strings.SplitN(s3path, "/", 2)
		uploads = append(uploads, &s3manager.UploadInput{
			Bucket: aws.String(bk[0]),
			Key:    aws.String(bk[1]),
			Body:   fp,
		})
		return nil
	})
	for i, found := range founds {
		if found {
			continue
		}
		if nofail {
			log.Println("local file not found for " + s3paths[i])
		} else {
			log.Fatal("local file not found for " + s3paths[i])
		}

	}
	return uploads, err
}

func Main() {

	// TODO: check Region with iid.
	cli := &cliargs{Processes: 2, Region: "us-east-1"}
	arg.MustParse(cli)
	cfg := aws.NewConfig().WithRegion(cli.Region)
	sess := session.Must(session.NewSession(cfg))
	svc := s3.New(sess)

	uploads, err := getupload(cli.S3Paths, svc, cli.Check, cli.NoFail)
	if err != nil {
		log.Fatal(err)
	}

	iter := make(chan *s3manager.UploadInput, len(uploads))
	for _, u := range uploads {
		iter <- u
	}
	close(iter)

	var wg sync.WaitGroup
	wg.Add(cli.Processes)

	for i := 0; i < cli.Processes; i++ {
		go func() {
			// NOTE: using multiple uploaders, each of which has concurrency. Might want to tune this later.
			uploader := s3manager.NewUploaderWithClient(svc, func(u *s3manager.Uploader) {
				u.PartSize = 16 * 1024 * 1024 // 64MB per part
				u.LeavePartsOnError = false
				u.Concurrency = 5
			})
			for u := range iter {

				t := time.Now()
				fmt.Fprintf(os.Stderr, "[batchit s3upload] starting upload of %s\n", u.Body.(*os.File).Name())

				_, err := uploader.Upload(u, func(u *s3manager.Uploader) {
					u.PartSize = 24 * 1024 * 1024 // 64MB per part
					u.LeavePartsOnError = false
				})
				if err != nil {
					log.Fatal(err)
				}
				fmt.Fprintf(os.Stderr, "[batchit s3upload] uploaded %s in %s\n", u.Body.(*os.File).Name(), time.Since(t))

			}
			wg.Done()
		}()
	}
	wg.Wait()

}
