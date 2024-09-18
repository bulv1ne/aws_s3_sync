package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"
	"github.com/schollz/progressbar/v3"
	"io"
	"iter"
	"os"
	"sync"
)

func main() {
	fs := ff.NewFlagSet("aws_s3_sync")
	sourceBucket := fs.StringLong("source-bucket", "", "Source bucket")
	destBucket := fs.StringLong("dest-bucket", "", "Dest bucket")
	sourceProfile := fs.StringLong("source-profile", "", "Source profile")
	destProfile := fs.StringLong("dest-profile", "", "Dest profile")
	prefix := fs.StringLong("prefix", "", "S3 Prefix")
	dryRun := fs.BoolLong("dryrun", "Dry run the operation")

	err := ff.Parse(
		fs,
		os.Args[1:],
		ff.WithEnvVarPrefix("AWS_S3_SYNC"),
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
	)
	if err != nil {
		fmt.Printf("%s\n", ffhelp.Flags(fs))
		fmt.Printf("err=%v\n", err)
		return
	}
	fmt.Printf(
		"Copy from s3://%s/%s to s3://%s/%s\n",
		*sourceBucket,
		*prefix,
		*destBucket,
		*prefix,
	)

	sourceS3Client := S3ClientFromProfile(*sourceProfile)
	destS3Client := S3ClientFromProfile(*destProfile)

	var sourceObjects []types.Object
	var totalSize int64
	for object := range NewSourceObjects(sourceS3Client, destS3Client, sourceBucket, destBucket, prefix) {
		sourceObjects = append(sourceObjects, object)
		totalSize += *object.Size
	}
	fmt.Println(len(sourceObjects), "objects to copy")
	fmt.Println(totalSize/1024, "KiB")

	if !*dryRun && totalSize > 0 {
		bar := progressbar.DefaultBytes(
			totalSize,
			"uploading",
		)
		jobs := make(chan CopyFileInput, 1)
		results := make(chan int64)

		const numWorkers = 50
		var wg sync.WaitGroup
		for i := 0; i < min(numWorkers, len(sourceObjects)); i++ {
			wg.Add(1)
			go worker(&wg, jobs, results)
		}
		go func() {
			for _, object := range sourceObjects {
				jobs <- CopyFileInput{
					sourceS3Client,
					destS3Client,
					sourceBucket,
					destBucket,
					object,
				}
			}
			close(jobs)
			wg.Wait()
			close(results)
		}()
		for size := range results {
			err := bar.Add(int(size))
			if err != nil {
				fmt.Printf("err=%v\n", err)
			}
		}
	}
}

func worker(wg *sync.WaitGroup, jobs <-chan CopyFileInput, results chan<- int64) {
	for copyFileInput := range jobs {
		size := *copyFileInput.object.Size
		err := CopyFile(
			context.Background(),
			copyFileInput,
		)

		results <- size
		if err != nil {
			panic(err)
		}
	}
	wg.Done()
}

func NewSourceObjects(sourceS3Client *s3.Client, destS3Client *s3.Client, sourceBucket *string, destBucket *string, prefix *string) iter.Seq[types.Object] {
	return func(yield func(types.Object) bool) {
		destObjects := make(map[string]types.Object)
		for object := range ListObjectsV2(destS3Client, &s3.ListObjectsV2Input{Bucket: destBucket, Prefix: prefix}) {
			destObjects[*object.Key] = object
		}

		for object := range ListObjectsV2(sourceS3Client, &s3.ListObjectsV2Input{Bucket: sourceBucket, Prefix: prefix}) {
			val, exists := destObjects[*object.Key]
			if !exists || *val.ETag != *object.ETag {
				if !yield(object) {
					return
				}
			}
		}
	}
}

type CopyFileInput struct {
	sourceS3Client *s3.Client
	destS3Client   *s3.Client
	sourceBucket   *string
	destBucket     *string
	object         types.Object
}

func CopyFile(ctx context.Context, params CopyFileInput) error {
	getObject, err := params.sourceS3Client.GetObject(ctx, &s3.GetObjectInput{Bucket: params.sourceBucket, Key: params.object.Key})
	if err != nil {
		return fmt.Errorf("couldn't GetObject err=%v", err)
	}
	bodyBytes, err := io.ReadAll(getObject.Body)
	if err != nil {
		return fmt.Errorf("couldn't ReadAll body err=%v", err)
	}
	err = getObject.Body.Close()
	if err != nil {
		return fmt.Errorf("couldn't close GetObject.Body err=%v", err)
	}

	_, err = params.destS3Client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket:      params.destBucket,
			Key:         params.object.Key,
			ContentType: getObject.ContentType,
			Body:        bytes.NewReader(bodyBytes),
		},
	)
	if err != nil {
		return fmt.Errorf("couldn't PutObject err=%v", err)
	}
	return nil
}

func S3ClientFromProfile(profileName string) *s3.Client {
	sdkConfig, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithSharedConfigProfile(profileName),
	)
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		panic(err)
	}
	return s3.NewFromConfig(sdkConfig)
}

func ListObjectsV2(s3Client *s3.Client, params *s3.ListObjectsV2Input) iter.Seq[types.Object] {
	return func(yield func(types.Object) bool) {
		kwargs := *params
		for {
			result, err := s3Client.ListObjectsV2(
				context.TODO(),
				&kwargs,
			)
			if err != nil {
				panic(err)
			}
			if !yieldSlice(yield, result.Contents) {
				return
			}

			if result.NextContinuationToken != nil {
				kwargs.ContinuationToken = result.NextContinuationToken
			} else {
				break
			}
		}
	}
}

func yieldSlice[T any](yield func(T) bool, slice []T) bool {
	for _, item := range slice {
		if !yield(item) {
			return false
		}
	}
	return true
}
