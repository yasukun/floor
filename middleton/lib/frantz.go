package lib

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo"
	"github.com/linkedin/goavro"
	kafka "github.com/segmentio/kafka-go"
)

// searchOffset ...
func searchOffset(c echo.Context) error {
	cc := c.(*CustomContext)
	filter := cc.Param("filter")
	alloffset, err := cc.Client.HGetAll("offset").Result()
	if err != nil {
		cc.Logger().Errorf("redisdb hgetall error: %v", err)
		return err
	}
	offsets := []Offset{}
	for k, v := range alloffset {
		fields := strings.Split(k, ":")
		partition, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			cc.Logger().Errorf("partiton convert error: %v", err)
			return err
		}
		offset, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			cc.Logger().Errorf("offset convert error: %v", err)
			return err
		}
		offsets = append(offsets, Offset{
			Topic:     fields[0],
			Partition: partition,
			Offset:    offset,
		})
	}
	filterd := []Offset{}
	switch filter {
	case "subject":
		for _, offset := range offsets {
			if offset.Topic == cc.Config.Subject.Topic {
				filterd = append(filterd, offset)
			}
		}
	default:
		filterd = offsets
	}
	return cc.JSON(http.StatusOK, filterd)
}

// searchKafka ...
func searchKafka(conf Config, codec *goavro.Codec, topic string, partition int, offset int64, rule Filter) []kafka.Message {
	brokers := []string{}
	for _, broker := range conf.Kafka.Brokers {
		brokers = append(brokers, broker.Addr)
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: partition,
		MinBytes:  conf.Kafka.Minbytes,
		MaxBytes:  conf.Kafka.Maxbytes,
		MaxWait:   time.Duration(conf.Kafka.MaxWait) * time.Second,
	})
	defer r.Close()
	r.SetOffset(offset - 1)
	msgs := []kafka.Message{}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(conf.Kafka.Cancel)*time.Second)
	defer cancel()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}
		if rule.Match(m) {
			msgs = append(msgs, m)
		}
	}
	return msgs
}

type Filter interface {
	Match(m kafka.Message) bool
}

type subjectRure struct {
	Id       string
	Offset   int64
	Category string
	Codec    *goavro.Codec
}

// NewSubjectRure ...
func NewSubjectRure(id, category string, codec *goavro.Codec) subjectRure {
	return subjectRure{Id: id, Offset: -1, Category: category, Codec: codec}
}

// (s subjectRure) Match ...
func (s *subjectRure) Match(m kafka.Message) bool {
	native, _, err := s.Codec.NativeFromBinary(m.Value)
	if err != nil {
		return false
	}

	if v, ok := native.(map[string]interface{})["id"]; ok {
		if v.(string) == s.Id {
			s.Offset = m.Offset
		}
	}

	if v, ok := native.(map[string]interface{})["category"]; ok {
		if s.Offset > 0 && v.(string) == s.Category {
			return true
		}
	}

	return false
}

// searchSubject ...
func searchSubject(c echo.Context) error {
	cc := c.(*CustomContext)
	category := cc.Param("category")
	id := cc.Param("xid")
	o := new([]Offset)
	if err := cc.Bind(o); err != nil {
		cc.Logger().Errorf("Bind error: %v", err)
		return err
	}

	queue := make(chan kafka.Message)
	result := []string{}
	wg := &sync.WaitGroup{}
	for _, subjectOffset := range *o {
		wg.Add(1)
		go func(conf Config, codec *goavro.Codec, topic string, partition int, offset int64, id, category string) {
			rule := NewSubjectRure(id, category, codec)
			msgs := searchKafka(conf, codec, topic, partition, offset, &rule)
			for _, msg := range msgs {
				queue <- msg
			}
			wg.Done()
		}(cc.Config, cc.Codecs.Subject, subjectOffset.Topic, int(subjectOffset.Partition), subjectOffset.Offset, id, category)
	}
	go func() {
		for q := range queue {
			result = append(result, string(q.Value))
		}
	}()
	wg.Wait()
	resp, err := responseSubject(cc.Codecs.Subject, &result)
	if err != nil {
		cc.Logger().Errorf("make respose error: %v", err)
	}
	cc.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSONCharsetUTF8)
	cc.Response().WriteHeader(http.StatusOK)

	return json.NewEncoder(c.Response()).Encode(resp)
}
