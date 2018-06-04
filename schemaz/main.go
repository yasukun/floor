// avro schema test and example
//
// First we will create an embedded file in the roure.avro directory
//
// ```
// $ go-bindata roure.avro/
// ```
//
package main

import (
	"encoding/json"
	"fmt"
	"log"

	"time"

	"github.com/rs/xid"
	goavro "gopkg.in/linkedin/goavro.v2"
)

// NewSubject ...
func NewSubject() Subject {
	guid := xid.New()

	cmds := []Command{}
	cmds = append(cmds, Command{
		Group: "LISTS",
		Key:   "sumbject:new",
		Value: "test value",
	})
	cmds = append(cmds, Command{
		Group: "HASHES",
		Key:   "subject:new",
		Field: "invert_idx:" + guid.String(),
		Value: "1",
	})

	tags := []Tag{}
	tags = append(tags, Tag{Name: "kenmo"})
	imgs := []Image{}
	imgs = append(imgs, Image{Src: "http://imgur.com/test.jpg"})

	uts := time.Now().Unix()
	subject := Subject{
		Id:          guid.String(),
		Category:    "news",
		Name:        "774",
		Uts:         uts,
		Host:        "127.0.0.1",
		FingerPrint: "12345",
		Body:        "main message",
		Url:         "http://google.co.jp",
		Redis:       cmds,
		Tags:        tags,
		Images:      imgs,
	}
	return subject
}

// SubjectSample ...
func SubjectSample() error {
	schema, err := Asset("roure.avro/subject.avsc")
	if err != nil {
		return err
	}
	codec, err := goavro.NewCodec(string(schema))
	if err != nil {
		return err
	}
	source, err := json.Marshal(NewSubject())
	if err != nil {
		return err
	}
	native, _, err := codec.NativeFromTextual(source)
	if err != nil {
		return err
	}
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return err
	}
	native, _, err = codec.NativeFromBinary(binary)
	if err != nil {
		return err
	}
	textual, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return err
	}
	fmt.Println(string(textual))
	return nil
}

func main() {

	log.Println("avro test")
	if err := SubjectSample(); err != nil {
		log.Fatalln("subject error: ", err)
	}

}
