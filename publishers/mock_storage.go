package publishers

import (
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
	return nil
}
