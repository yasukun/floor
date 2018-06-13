package lib

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/linkedin/goavro"
	"github.com/rs/xid"
	kafka "github.com/segmentio/kafka-go"
)

// newSubjectWriter ...
func newSubjectWriter(conf Config) *kafka.Writer {
	brokers := []string{}
	for _, broker := range conf.Kafka.Brokers {
		brokers = append(brokers, broker.Addr)
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        conf.Subject.Topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: conf.Subject.Ack,
		WriteTimeout: time.Duration(conf.Kafka.WriteTimeout) * time.Second,
	})
	return w
}

// newSubjectMsg ...
func newSubjectMsg(codec *goavro.Codec, category, host string, subject *Subject) (msg kafka.Message, resp PostResponse, err error) {
	guid := xid.New()
	resp = PostResponse{Type: "subject", Category: category, Id: guid.String()}
	uts := time.Now().Unix()
	subject.Category = category
	subject.Id = guid.String()
	subject.Host = host
	subject.Uts = uts
	cmds := []Command{}
	cmds = append(cmds, Command{
		Group: "LISTS",
		Key:   SubjectKey(category),
		From:  "SELF",
		Value: "",
	}, Command{
		Group: "HASHES",
		Key:   SubjectKey(category),
		Field: SubjectDetailKey(guid.String()),
		From:  "PREVIOUS_VALUE",
		Value: "",
	}, Command{
		Group: "ZADD",
		Key:   SubjectKey(category),
		From:  "VALUE",
		Value: guid.String(),
	})
	subject.Redis = cmds
	if subject.Images == nil {
		subject.Images = []Image{}
	}
	if subject.Tags == nil {
		subject.Tags = []Tag{}
	}
	jsonB, err := json.Marshal(subject)
	if err != nil {
		return
	}

	native, _, err := codec.NativeFromTextual(jsonB)
	if err != nil {
		return
	}
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return
	}
	msg = kafka.Message{
		Key:   []byte(category),
		Value: binary,
	}
	return
}

// SubjectKey ...
func SubjectKey(category string) string {
	return "subject:" + category
}

// SubjectDetailKey ...
func SubjectDetailKey(id string) string {
	return "Inverted:" + id
}

// responseSubject ...
func responseSubject(codec *goavro.Codec, subjects *[]string) (*[]interface{}, error) {
	resp := []interface{}{}
	for _, binary := range *subjects {
		native, _, err := codec.NativeFromBinary([]byte(binary))
		if err != nil {
			return &resp, err
		}
		resp = append(resp, native)
	}
	return &resp, nil
}
