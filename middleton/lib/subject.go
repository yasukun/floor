package lib

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/labstack/echo"
	"github.com/linkedin/goavro"
	"github.com/rs/xid"
	kafka "github.com/segmentio/kafka-go"
)

// lenSubject ...
func lenSubject(c echo.Context) error {
	cc := c.(*CustomContext)
	key := CommentKey(cc.Param("subjec_id"))
	i64, err := llen(cc.Client, key)
	if err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: "llen error",
		})
	}
	return cc.JSON(http.StatusOK, &IntResponse{Result: i64})
}

// newSubjectMsg ...
func newSubjectMsg(codec *goavro.Codec, category, host string, subject *Subject) (msg kafka.Message, resp PostResponse, err error) {
	guid := xid.New()
	resp = PostResponse{Type: "subject", Category: category, ID: guid.String()}
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

// rangeSubject ...
func rangeSubject(c echo.Context) error {
	cc := c.(*CustomContext)
	k := SubjectKey(cc.Param("category"))
	limit := int64(cc.Config.Subject.Limit)
	r := new(PostRange)
	if err := cc.Bind(r); err != nil {
		cc.Logger().Errorf("Bind error: %v", err)
		return cc.JSON(http.StatusInternalServerError, ErrResponse{
			Message: fmt.Sprintf("bind subject error: %v", err),
		})
	}

	if r.Start > r.Stop {
		return cc.JSON(http.StatusInternalServerError, ErrResponse{
			Message: "range error",
		})
	}

	if r.Stop-r.Start > limit {
		return cc.JSON(http.StatusInternalServerError, ErrResponse{
			Message: "range over limit",
		})
	}

	subjects, err := cc.Client.LRange(k, r.Start, r.Stop).Result()
	if err != nil {
		return errors.New(fmt.Sprintf("range subject error: %v", err))
	}
	s, err := responseSubject(cc.Codecs.Subject, &subjects)
	if err != nil {
		return errors.New(fmt.Sprintf("build subject response error: %v", err))
	}
	cc.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSONCharsetUTF8)
	cc.Response().WriteHeader(http.StatusOK)

	return json.NewEncoder(c.Response()).Encode(s)

}

// latestSubject ...
func latestSubject(c echo.Context) error {
	cc := c.(*CustomContext)
	k := SubjectKey(cc.Param("category"))
	limit := int64(cc.Config.Subject.Limit)
	subjects, err := cc.Client.LRange(k, limit*-1, -1).Result()
	if err != nil {
		return errors.New(fmt.Sprintf("laitest subject error: %v", err))
	}
	s, err := responseSubject(cc.Codecs.Subject, &subjects)
	if err != nil {
		return errors.New(fmt.Sprintf("build subject response error: %v", err))
	}
	cc.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSONCharsetUTF8)
	cc.Response().WriteHeader(http.StatusOK)

	return json.NewEncoder(c.Response()).Encode(s)
}

// detailSubject ...
func detailSubject(c echo.Context) error {
	cc := c.(*CustomContext)
	k := SubjectKey(cc.Param("category"))
	f := SubjectDetailKey(cc.Param("xid"))
	size, err := cc.Client.HGet(k, f).Result()
	if err != nil {
		return errors.New(fmt.Sprintf("detail subject size error: %v", err))
	}
	i64, err := strconv.ParseInt(size, 10, 64)
	idx := i64 - 1
	detail, err := cc.Client.LIndex(k, idx).Result()
	if err != nil {
		return errors.New(fmt.Sprintf("detail subject error: %v", err))
	}
	native, _, err := cc.Codecs.Subject.NativeFromBinary([]byte(detail))
	if err != nil {
		return errors.New(fmt.Sprintf("decode subject detail error: %v", err))
	}

	return cc.JSON(http.StatusOK, native)
}

// indexSubjec ...
func indexSubject(c echo.Context) error {
	cc := c.(*CustomContext)
	k := SubjectKey(cc.Param("category"))
	f := SubjectDetailKey(cc.Param("xid"))
	idx, err := cc.Client.HGet(k, f).Int64()
	if err != nil {
		return err
	}
	resp := &IntResponse{Result: idx}
	return cc.JSON(http.StatusOK, resp)
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

// newSubject ...
func newSubject(c echo.Context) error {
	cc := c.(*CustomContext)
	s := new(Subject)
	cc.Logger().Info("newsubject")
	if err := cc.Bind(s); err != nil {
		cc.Logger().Errorf("Bind error: %v", err)
		return err
	}
	category := cc.Param("category")
	host := cc.Request().Header.Get("X-Forwarded-For")

	msg, resp, err := newSubjectMsg(cc.Codecs.Subject, category, host, s)
	if err != nil {
		cc.Logger().Errorf("create subject msg error: %v", err)
		return err
	}
	if err := produceMsg(&cc.Config, cc.Config.Subject.Topic, cc.Config.Subject.Ack, &msg); err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("produce msg error: %v", err),
		})
	}
	return cc.JSON(http.StatusOK, resp)
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
