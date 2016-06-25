package publishers

import (
	"io"
	"log"
	"os"
	"path"

	"github.com/yewno/acquisition/services"
)

type MockStorage struct{}

func (z *MockStorage) Get(o *services.Object) error {

	p := path.Join(o.Bucket, o.Key)
	file, err := os.Open(p)
	if err != nil {
		return err
	}

	o.File = file

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	o.Size = stat.Size()

	return nil
}

func (z *MockStorage) Put(o *services.Object) error {

	file, err := os.OpenFile(path.Join(o.Bucket, o.Key), os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Println(err)
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, o.File)
	if err != nil {
		return err
	}
	return nil
}
