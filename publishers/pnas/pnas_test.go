package pnas

import (
	"log"
	"testing"

	"github.com/yewno/acquisition/publishers"
)

func TestProcess(t *testing.T) {

	conn := &publishers.MockStorage{}

	obj := &Object{
		conn: conn,
	}

	err := obj.Process("../../test", "files/pnas/113_1/pnas_113_1.pdf.zip")

	log.Println(err)
}
