package pnas

import (
	"archive/zip"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"

	"golang.org/x/net/html/charset"

	"github.com/yewno/acquisition/services"
)

var extensions = []string{".xml.zip", ".pdf.zip"}

type Object struct {
	conn services.CobaltStorage
	wg   *sync.WaitGroup
}

func (o *Object) Process(bucket, key string) error {

	object := services.NewObject(nil, bucket, key, 0)

	if !contains(object.Key, extensions) {
		return nil
	}

	err := o.conn.Get(object)
	if err != nil {
		return err
	}

	reader, err := zip.NewReader(object.File, object.Size)
	if err != nil {
		return err
	}

	_, filename := path.Split(object.Key)
	parts := strings.Split(filename, "_")

	volume := parts[1]

	for _, file := range reader.File {

		if file.FileInfo().IsDir() {
			continue
		}

		page, err := getPage(file.FileInfo().Name())
		if err != nil {
			continue
		}

		ext := path.Ext(file.Name)
		switch ext {

		case ".xml":
			issue := strings.TrimSuffix(parts[len(parts)-1], ".xml.zip")
			name := fmt.Sprintf("%s/pnas_v%s_i%s_p%s%s", "files/pnas", volume, issue, page, ".xml")
			err = o.handleXML(file, name)

		case ".pdf":
			issue := strings.TrimSuffix(parts[len(parts)-1], ".pdf.zip")
			name := fmt.Sprintf("%s/pnas_v%s_i%s_p%s%s", "files/pnas", volume, issue, page, ".pdf")
			err = o.handlePDF(file, name)

		default:
			return errors.New(fmt.Sprintf("(%s) Extension handler missing for (%s)", "PNAS", ext))

		}
	}
	return nil
}

type record struct {
	Volume string `xml:"volume"`
	Issue  string `xml:"issue"`
	Page   string `xml:"fpage"`
}

func (o *Object) handleXML(file *zip.File, key string) error {
	reader, err := file.Open()
	if err != nil {
		return err
	}

	decoder := xml.NewDecoder(reader)
	decoder.CharsetReader = charset.NewReaderLabel
	var meta record
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

		temp, size, err := ZipEntryToFile(reader)
		if err != nil {
			return err
		}

		object := services.NewObject(temp, YewnoProcessedBucket, key, size)

		err = object.Save(o.conn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Object) handlePDF(file *zip.File, key string) error {
	reader, err := file.Open()
	if err != nil {
		return err
	}

	temp, size, err := ZipEntryToFile(reader)
	if err != nil {
		return err
	}

	object := services.NewObject(temp, YewnoProcessedBucket, key, size)

	err = object.Save(o.conn)
	if err != nil {
		return err
	}

	return nil
}

func ZipEntryToFile(reader io.Reader) (*os.File, int64, error) {

	temp, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, -1, err
	}

	size, err := io.Copy(temp, reader)
	return temp, size, err
}

func contains(s string, sa []string) bool {

	for _, ext := range sa {
		if strings.HasSuffix(s, ext) {
			return true
		}
	}
	return false
}

func trimToFirstInt(in string) string {
	var index int
	for _, v := range in {
		switch {
		case v >= '0' && v <= '9':
			return in[index:len(in)]
		default:
			index++
		}
	}
	return in
}

func getPage(filename string) (string, error) {
	trimmedFilename := trimToFirstInt(filename)
	pageParts := strings.Split(trimmedFilename, ".")
	page := pageParts[0]

	if page == "" || page == "xml" {
		return "", errors.New("No pages found")
	}

	trimVal := 6
	if len(page) < 6 {
		trimVal = len(page)
	}

	page = page[len(page)-trimVal : len(page)]
	page = strings.Replace(page, "-", "", -1)
	if strings.HasPrefix(page, "q") {
		page = page[1:len(page)]
	}

	page = strings.TrimLeft(page, "0")
	if len(page) < 5 {
		page = strings.Repeat("0", 5-len(page)) + page
	}

	page = page[len(page)-5 : len(page)]
	return page, nil
}
