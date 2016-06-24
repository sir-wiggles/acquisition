package pnas

import (
	"log"
	"testing"

	"github.com/yewno/acquisition/publishers"
)

func TestProcess(t *testing.T) {

	conn := &publishers.MockStorage{}

	err := Process(conn, "../../test", "files/pnas/113_1/pnas_113_1.xml.zip")

	log.Println(err)
}
