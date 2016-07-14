package acm

import (
	"bytes"
	"compress/gzip"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"sync"

	"golang.org/x/net/html/charset"

	"github.com/yewno/acquisition/config"
	"github.com/yewno/acquisition/publishers"
	"github.com/yewno/acquisition/services"
)

type Object struct {
	conn  services.CobaltStorage
	queue services.CobaltQueue
	wg    *sync.WaitGroup
	cfg   *config.Config
	pool  chan bool

	stats *publishers.Stats
}

func NewObject(conn services.CobaltStorage, queue services.CobaltQueue, wg *sync.WaitGroup, cfg *config.Config, pool chan bool, stats *publishers.Stats) *Object {
	wg.Add(1)
	return &Object{
		conn:  conn,
		queue: queue,
		wg:    wg,
		cfg:   cfg,
		pool:  pool,

		stats: stats,
	}
}

func getBaseKey(filename string) string {
	fileParts := strings.Split(filename, "/")
	filename = fileParts[len(fileParts)-1]
	fileParts = strings.Split(filename, ".")
	return fileParts[0]
}

func (o *Object) UploadACM(key string, bytesArr []byte) error {

	file, err := ioutil.TempFile("", "")
	if err != nil {
		log.Println(err)
		return err
	}

	size, err := file.Write(bytesArr)
	if err != nil {
		log.Println(err)
		return err
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		log.Println(err)
		return err
	}

	object := services.NewObject(file, o.cfg.ProcessedBucket, key, int64(size))

	err = object.Save(o.conn)
	if err != nil {
		log.Println(err)
	}
	object.Close()

	return err
}

func (o *Object) ProcessProceeding(filename string, bytesArr []byte) ([]string, error) {
	v := new(ACMProceedings)
	decoder := xml.NewDecoder(bytes.NewReader(bytesArr))
	decoder.CharsetReader = charset.NewReaderLabel

	keys := make([]string, 0, 100)
	if err := decoder.Decode(v); err != nil {
		return keys, err
	}

	baseName := getBaseKey(filename)

	for _, section := range v.Sections {

		for _, article := range section.Articles {
			var p ACMProceeding
			p.Conference = v.Conference
			p.Proceeding = v.Proceeding
			p.SectionID = section.ID
			p.Title = section.Title
			p.Type = section.Type
			p.Article = article
			bytesArr, err := xml.Marshal(p)

			if err != nil {
				return keys, err
			}

			key := fmt.Sprintf("acm/%s-%s.xml", baseName, p.Article.ID)
			err = o.UploadACM(key, bytesArr)

			if err != nil {
				return keys, err
			}
			keys = append(keys, key)
		}
	}

	for _, article := range v.Articles {
		var p ACMProceeding
		p.Conference = v.Conference
		p.Proceeding = v.Proceeding
		p.Article = article
		bytesArr, err := xml.Marshal(p)

		if err != nil {
			return keys, err
		}

		key := fmt.Sprintf("acm/%s-%s.xml", baseName, p.Article.ID)
		err = o.UploadACM(key, bytesArr)

		if err != nil {
			return keys, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (o *Object) ProcessPeriodical(filename string, bytesArr []byte) ([]string, error) {
	v := new(ACMPeriodicals)
	keys := make([]string, 0, 100)
	decoder := xml.NewDecoder(bytes.NewReader(bytesArr))
	decoder.CharsetReader = charset.NewReaderLabel
	if err := decoder.Decode(v); err != nil {
		return nil, err
	}

	baseName := getBaseKey(filename)
	for _, section := range v.Sections {

		for _, article := range section.Articles {

			var p ACMPeriodical
			p.Journal = v.Journal
			p.Issue = v.Issue
			p.SectionID = section.ID
			p.Title = section.Title
			p.Type = section.Type
			p.Article = article
			bytesArr, err := xml.Marshal(p)

			if err != nil {
				return keys, err
			}

			key := fmt.Sprintf("acm/%s-%s.xml", baseName, p.Article.ID)
			err = o.UploadACM(key, bytesArr)

			if err != nil {
				return keys, err
			}
			keys = append(keys, key)
		}
	}

	for _, article := range v.Articles {
		var p ACMPeriodical
		p.Journal = v.Journal
		p.Issue = v.Issue
		p.Article = article
		bytesArr, err := xml.Marshal(p)

		if err != nil {
			return keys, err
		}

		key := fmt.Sprintf("acm/%s-%s.xml", baseName, p.Article.ID)
		err = o.UploadACM(key, bytesArr)

		if err != nil {
			return keys, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (o *Object) Process(receipt string, message *services.SnsMessage) error {

	defer o.wg.Done()
	defer func() { <-o.pool }()

	var (
		bucket = message.Records[0].S3.Bucket.Name
		key    = message.Records[0].S3.Object.Key
		size   = message.Records[0].S3.Object.Size
		keys   = make([]string, 0, 100)
	)
	log.Println(bucket, key, size)

	object := services.NewObject(nil, bucket, key, size)

	if err := o.conn.Get(object); err != nil {
		return err
	}
	defer object.Close()

	reader, err := gzip.NewReader(object.File)
	if err != nil {
		return err
	}

	bytesArr, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	reader.Close()
	//bytesArr = carbon.CleanXML(bytesArr)

	if bytes.Contains(bytesArr, []byte("<periodical ")) {
		keys, err = o.ProcessPeriodical(key, bytesArr)
		if err != nil {
			return err
		}
	} else if bytes.Contains(bytesArr, []byte("<proceeding ")) {
		keys, err = o.ProcessProceeding(key, bytesArr)
		if err != nil {
			return err
		}
	} else {
		err = errors.New("unknown format")
		return err
	}

	batch := o.queue.NewBatch(o.cfg.ProcessedQueue)
	count := 0
	for _, key := range keys {
		m := services.PairMessage{
			Source: "acm",
			Bucket: o.cfg.ProcessedBucket,
			Key:    fmt.Sprintf("%s.gz", key),
		}
		batch.Add(m)
		count++
	}
	o.stats.Report <- fmt.Sprintf("acm:%d", count)
	batch.Flush()

	if err = o.queue.Pop(o.cfg.NewContentQueue, receipt); err != nil {
		log.Println(err)
		return err
	}
	return nil
}

type ACMArticle struct {
	ID           string `xml:"article_id"`
	Title        string `xml:"title"`
	SubTitle     string `xml:"subtitle"`
	Date         string `xml:"article_publication_date"`
	StartPage    string `xml:"page_from"`
	EndPage      string `xml:"page_to"`
	ManuscriptId string `xml:"manuscript_tracking_id"`
	DOI          string `xml:"doi_number"`
	URL          string `xml:"url"`
	Abstract     struct {
		Text string `xml:",innerxml"`
	} `xml:"abstract>par"`
	Keywords []struct {
		Text string `xml:",innerxml"`
	} `xml:"keywords>kw"`
	// Categories struct {
	// 	// Primary
	// 	//Other
	// 	}`xml:"categories"`
	Authors []struct {
		ID         string `xml:"person_id"`
		Seq        string `xml:"seq_no"`
		FirstName  string `xml:"first_name"`
		LastName   string `xml:"last_name"`
		MiddleName string `xml:"middle_name"`
		Suffix     string `xml:"suffix"`
		Role       string `xml:"role"`
	} `xml:"authors>au"`
	Body []struct {
		Text string `xml:",innerxml"`
	} `xml:"fulltext>ft_body"`
}

type ACMPublisher struct {
	ID   string `xml:"publisher_id"`
	Code string `xml:"publisher_code"`
	Name string `xml:"publisher_name"`
	URL  string `xml:"publisher_url"`
}

type ACMSection struct {
	ID    string `xml:"section_id"`
	Title string `xml:"section_title"`
	Type  string `xml:"section_type"`
	//chair_editor
	Articles []ACMArticle `xml:"article_rec"`
}
type ACMJournal struct {
	ID        string       `xml:"journal_id"`
	Code      string       `xml:"journal_code"`
	Name      string       `xml:"journal_name"`
	Abbr      string       `xml:"journal_abbr"`
	ISSN      string       `xml:"issn"`
	EISSN     string       `xml:"eissn"`
	Language  string       `xml:"language"`
	Type      string       `xml:"periodical_type"`
	Publisher ACMPublisher `xml:"publisher"`
}

type ACMIssue struct {
	ID     string `xml:"issue_id"`
	Volume string `xml:"volume"`
	Issue  string `xml:"issue"`
	Date   string `xml:"publication_date"`
}

// periodical.dtd
type ACMPeriodical struct {
	Journal   ACMJournal `xml:"journal_rec"`
	Issue     ACMIssue   `xml:"issue_rec"`
	SectionID string     `xml:"section_id"`
	Title     string     `xml:"section_title"`
	Type      string     `xml:"section_type"`
	Article   ACMArticle `xml:"article_rec"`
}

// periodical.dtd RAW
type ACMPeriodicals struct {
	Journal  ACMJournal   `xml:"journal_rec"`
	Issue    ACMIssue     `xml:"issue_rec"`
	Sections []ACMSection `xml:"content>section"`
	Articles []ACMArticle `xml:"content>article_rec"`
}

type ACMConference struct {
	StartDate string `xml:"conference_date>start_date"`
	EndDate   string `xml:"conference_date>end_date"`
	URL       string `xml:"conference_url"`
	Location  struct {
		City    string `xml:"city"`
		State   string `xml:"state"`
		Country string `xml:"country"`
	} `xml:"conference_loc"`
}

type ACMProceed struct {
	ISBN        string `xml:"isbn"`
	ISBN13      string `xml:"isbn13"`
	ISSN        string `xml:"issn"`
	EISSN       string `xml:"eissn"`
	Date        string `xml:"publication_date"`
	Title       string `xml:"proc_title"`
	Description string `xml:"proc_desc"`
	Abstract    struct {
		Text string `xml:",innerxml"`
	} `xml:"abstract>par"`
	Publisher ACMPublisher `xml:"publisher"`
	Copyright struct {
		Holder string `xml:"copyright_holder_name"`
		Year   string `xml:"copyright_holder_year"`
	} `xml:"ccc>copyright_holder"`
}

type ACMProceeding struct {
	Conference ACMConference `xml:"conference_rec"`
	Proceeding ACMProceed    `xml:"proceeding_rec"`
	SectionID  string        `xml:"section_id"`
	Title      string        `xml:"section_title"`
	Type       string        `xml:"section_type"`

	Article ACMArticle `xml:"article_rec"`
}

// proceeding.dtd
type ACMProceedings struct {
	Conference ACMConference `xml:"conference_rec"`
	//Series `xml:"series_rec>series_name"`
	Proceeding ACMProceed   `xml:"proceeding_rec"`
	Sections   []ACMSection `xml:"content>section"`
	Articles   []ACMArticle `xml:"content>article_rec"`
}
