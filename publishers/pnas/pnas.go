package pnas

import (
	"archive/zip"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"path"
	"strings"
	"time"

	"golang.org/x/net/html/charset"

	"github.com/yewno/acquisition/services"
)

var extensions = []string{".xml.zip", ".pdf.zip"}

func Process(conn services.CobaltStorage, bucket, key string) error {

	object := services.NewObject(nil, bucket, key, 0)

	if !contains(object.Key, extensions) {
		return nil
	}

	err := conn.Get(object)
	if err != nil {
		return err
	}

	reader, err := zip.NewReader(object.File, object.Size)
	if err != nil {
		return err
	}

	//_, filename := path.Split(object.Key)
	//parts := strings.Split(filename, "_")

	//volume := parts[1]
	//issue := strings.TrimSuffix(parts[len(parts)-1], ".xml.zip")

	for _, file := range reader.File {

		if file.FileInfo().IsDir() {
			continue
		}

		ext := path.Ext(file.Name)
		switch ext {
		case ".xml":
			err = handleXML(file)
		case ".pdf":
			err = handlePDF(file)
		default:
			return errors.New(fmt.Sprintf("(%s) Extension handler missing for (%s)", "PNAS", ext))

		}
	}
	return nil
}

type Meta struct {
	Volume string `xml:"volume"`
	Issue  string `xml:"issue"`
	Page   string `xml:"fpage"`
}

func handleXML(file *zip.File) error {
	reader, err := file.Open()
	if err != nil {
		return err
	}

	decoder := xml.NewDecoder(reader)
	decoder.CharsetReader = charset.NewReaderLabel
	var meta Meta
	for {
		t, err := decoder.Token()

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		switch se := t.(type) {
		case xml.StartElement:
			if se.Name.Local == "article-meta" {
				err := decoder.DecodeElement(&meta, &se)
				if err != nil {
					break
				}

			}
		}
	}
	log.Println(meta)
	time.Sleep(1 * time.Second)
	return nil
}

func handlePDF(file *zip.File) error {
	return nil
}

func contains(s string, sa []string) bool {

	for _, ext := range sa {
		if strings.HasSuffix(s, ext) {
			return true
		}
	}
	return false
}
