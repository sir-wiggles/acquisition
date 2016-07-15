package services

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/yewno/acquisition/config"
)

func NewCobaltS3(cfg *config.Config) (CobaltStorage, error) {

	var conn *s3.S3
	if cfg.Key == "" && cfg.Secret == "" {
		conn = s3.New(session.New(&aws.Config{
			Region: aws.String(cfg.Region),
		}))
	} else {
		conn = s3.New(session.New(&aws.Config{
			Region:      aws.String(cfg.Region),
			Credentials: credentials.NewStaticCredentials(cfg.Key, cfg.Secret, ""),
		}))
	}

	return &CobaltS3{
		Conn: conn,
	}, nil
}

type CobaltS3 struct {
	Conn *s3.S3
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

	_, err = object.File.Seek(0, 0)

	return err
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
	var err error

	switch path.Ext(o.Key) {
	case ".xml":
		err = o.compress()
	}

	if err != nil {
		return err
	}

	log.Printf("Uploading: %s > %s", o.Key, o.Bucket)
	return conn.Put(o)
}

func (o *Object) Close() error {
	err := o.File.Close()
	if err != nil {
		log.Println(err)
	}
	err = os.Remove(o.File.Name())

	if err != nil {
		log.Println(err)
	}
	return err
}

func (o *Object) compress() error {

	temp, err := ioutil.TempFile("", "")
	if err != nil {
		return err
	}

	writer := gzip.NewWriter(temp)

	_, err = io.Copy(writer, o.File)
	if err != nil {
		return err
	}

	o.File = temp
	o.Key = fmt.Sprintf("%s.gz", o.Key)

	if err = writer.Close(); err != nil {
		return err
	}

	_, err = temp.Seek(0, 0)
	return err
}
