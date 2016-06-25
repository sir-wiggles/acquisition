package services

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func NewCobaltS3(key, secret, region, bucket string) (CobaltStorage, error) {

	var conn *s3.S3
	if key == "" && secret == "" {
		conn = s3.New(session.New(&aws.Config{
			Region: aws.String(region),
		}))
	} else {
		conn = s3.New(session.New(&aws.Config{
			Region:      aws.String(region),
			Credentials: credentials.NewStaticCredentials(key, secret, ""),
		}))
	}

	return &CobaltS3{
		Conn:   conn,
		bucket: bucket,
	}, nil
}

type CobaltS3 struct {
	Conn   *s3.S3
	bucket string
}

func (c *CobaltS3) Get(object *Object) error {

	params := s3.GetObjectInput{
		Bucket: aws.String(object.Bucket),
		Key:    aws.String(object.Key),
	}

	resp, err := c.Conn.GetObject(&params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	file, err := ioutil.TempFile("", "")
	if err != nil {
		return err
	}

	size, err := io.Copy(file, resp.Body)
	if err != nil {
		return err
	}

	object.File = file
	object.Size = size

	return nil
}

func (c *CobaltS3) Put(object *Object) error {

	params := s3.PutObjectInput{
		Bucket: aws.String(object.Bucket),
		Key:    aws.String(object.Key),
		Body:   object.File,
	}

	_, err := c.Conn.PutObject(&params)
	if err != nil {
		return err
	}
	return nil
}

type Object struct {
	File   *os.File
	Bucket string
	Key    string
	Size   int64
}

func NewObject(file *os.File, bucket, key string, size int64) *Object {
	return &Object{
		File:   file,
		Bucket: bucket,
		Key:    key,
		Size:   size,
	}
}

func (o *Object) Save(conn CobaltStorage) error {
	return conn.Put(o)
}
