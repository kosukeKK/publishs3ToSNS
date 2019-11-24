package main

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"
	"golang.org/x/xerrors"
	"log"
	"os"
)

type Key struct {
	Key string
}

func GetKeys(svc *s3.S3, keys []Key, continuationToken *string) []Key {
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:            aws.String(os.Getenv("BUCKET")),
		Prefix:            aws.String(""),
		ContinuationToken: continuationToken,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, item := range resp.Contents {
		key := *item.Key
		if key[len(key)-1:] == "/" {
			continue
		}
		keys = append(keys, Key{
			Key: *item.Key,
		})
	}
	if resp.ContinuationToken != nil {
		return GetKeys(svc, keys, continuationToken)
	}
	return keys
}
func main() {
	creds := credentials.NewStaticCredentials(os.Getenv("AWS_ACCESS_KEY"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")
	sess, err := session.NewSession(&aws.Config{
		Credentials: creds,
		Region:      aws.String("ap-northeast-1"),
	})
	if err != nil {
		log.Fatal(err)
	}
	keys := GetKeys(s3.New(sess), []Key{}, nil)
	if err := sendToSNS(sns.New(sess), keys); err != nil {
		log.Fatal(err)
	}

}

func sendToSNS(svc *sns.SNS, keys []Key) error {
	message, err := json.Marshal(keys)
	if err != nil {
		return xerrors.Errorf("failed in sendToSNS: %w", err)
	}
	_, err = svc.Publish(&sns.PublishInput{
		Message:  aws.String(string(message)),
		TopicArn: aws.String(os.Getenv("TOPIC_ARN")),
	})
	if err != nil {
		return xerrors.Errorf("failed in sendToSNS: %w", err)
	}
	return nil
}
