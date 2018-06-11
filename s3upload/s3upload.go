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
	"github.com/pkg/errors"

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

var Found = errors.New("file found")

func upload(s3path string, region string, check bool, nofail bool) error {
	if strings.HasPrefix(s3path, "s3://") {
		s3path = s3path[5:]
	}
	bk := strings.SplitN(s3path, "/", 2)

	tmp := strings.Split(s3path, "/")
	localpath := tmp[len(tmp)-1]

	err := filepath.Walk(".", func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		tmp := strings.Split(f.Name(), "/")
		if tmp[len(tmp)-1] == localpath {
			cfg := aws.NewConfig().WithRegion(region)
			sess := session.Must(session.NewSession(cfg))
			svc := s3.New(sess)
			if check {
				// check if file exists in s3
				exists, size, err := submit.OutputExists(svc, s3path)
				if err != nil && err != submit.NotFound {
					return err
				}
				if err == nil && exists && size == f.Size() {
					fmt.Fprintf(os.Stderr, "[batchit s3uploader] %s already in s3, skipping\n", f.Name())
					return Found
				}

			}
			uploader := s3manager.NewUploaderWithClient(svc, func(u *s3manager.Uploader) {
				u.PartSize = 16 * 1024 * 1024 // 64MB per part
				u.Concurrency = 5
			})

			fp, err := os.Open(f.Name())
			if err != nil {
				return err
			}
			defer fp.Close()

			fmt.Fprintf(os.Stderr, "[batchit s3uploader] starting upload of %s\n", f.Name())
			t := time.Now()

			_, err = uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(bk[0]),
				Key:    aws.String(bk[1]),
				Body:   fp,
			})
			if err != nil {
				log.Println(err, "error uploading: %s", f.Name())
				return errors.Wrapf(err, "error uploading: %s", f.Name())
			}
			fmt.Fprintf(os.Stderr, "[batchit s3uploader] uploaded %s in %s\n", f.Name(), time.Since(t))
			return Found
		}
		return nil
	})
	if err == Found {
		return nil
	}
	if err == nil {
		return errors.Wrapf(submit.NotFound, s3path)
	}
	return err
}

func Main() {

	// TODO: check Region with iid.
	cli := &cliargs{Processes: 2, Region: "us-east-1"}
	arg.MustParse(cli)

	paths := make(chan string, len(cli.S3Paths))
	for _, p := range cli.S3Paths {
		paths <- p
	}
	close(paths)

	wg := &sync.WaitGroup{}
	for i := 0; i < cli.Processes; i++ {
		wg.Add(1)
		go func() {
			for path := range paths {
				if err := upload(path, cli.Region, cli.Check, cli.NoFail); err != nil {
					if cli.NoFail {
						log.Println(err)
						if len(cli.S3Paths) > 1 {
							log.Println("continuing with other uploads")
						}
					} else {
						log.Fatal(err)
					}
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	// always has non-zero exit status.
}
