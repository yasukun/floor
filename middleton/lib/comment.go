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

// CommentKey ...
func CommentKey(subjectID string) string {
	return "comment:subjectid:" + subjectID
}

// CommentDetailKey ...
func CommentDetailKey(id string) string {
	return "inverted:comment:" + id
}

// detailComments ...
func detailComments(c echo.Context) error {
	cc := c.(*CustomContext)
	k := CommentKey(cc.Param("subject_id"))
	p := new([]PostID)
	if err := cc.Bind(p); err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("bind PostIDs error: %v", err),
		})
	}
	idxs := []int64{}
	for _, postID := range *p {
		f := CommentDetailKey(postID.ID)
		size, err := cc.Client.HGet(k, f).Result()
		if err != nil {
			return cc.JSON(http.StatusBadRequest, ErrResponse{
				Message: "get comment idx  error",
			})
		}
		i64, err := strconv.ParseInt(size, 10, 64)
		idx := i64 - 1
		idxs = append(idxs, idx)
	}
	details := []interface{}{}
	for _, idx := range idxs {
		detail, err := cc.Client.LIndex(k, idx).Result()
		if err != nil {
			return cc.JSON(http.StatusBadRequest, ErrResponse{
				Message: fmt.Sprintf("get comment detail  error: %v", err),
			})
		}
		native, _, err := cc.Codecs.Comment.NativeFromBinary([]byte(detail))
		if err != nil {
			return cc.JSON(http.StatusBadRequest, ErrResponse{
				Message: fmt.Sprintf("decode comment detail  error: %v", err),
			})
		}
		details = append(details, native)
	}
	return cc.JSON(http.StatusOK, details)
}

// rangeComment ...
func rangeComment(c echo.Context) error {
	cc := c.(*CustomContext)
	key := CommentKey(cc.Param("subject_id"))
	limit := int64(cc.Config.Comment.Limit)
	r := new(PostRange)
	if err := cc.Bind(r); err != nil {
		cc.Logger().Errorf("Bind error: %v", err)
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("bind subject error: %v", err),
		})
	}
	if r.Start > r.Stop {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: "range error",
		})
	}
	if r.Stop-r.Start > limit {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: "range over limit",
		})
	}
	comment, err := cc.Client.LRange(key, r.Start, r.Stop).Result()
	if err != nil {
		return errors.New(fmt.Sprintf("range comment error: %v", err))
	}
	resp := []interface{}{}
	for _, binary := range comment {
		native, _, err := cc.Codecs.Comment.NativeFromBinary([]byte(binary))
		if err != nil {
			return cc.JSON(http.StatusBadRequest, ErrResponse{
				Message: fmt.Sprintf("convert binary to native error: %v", err),
			})
		}
		resp = append(resp, native)
	}
	cc.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSONCharsetUTF8)
	cc.Response().WriteHeader(http.StatusOK)

	return json.NewEncoder(c.Response()).Encode(resp)
}

// newComment ...
func newComment(c echo.Context) error {
	cc := c.(*CustomContext)
	comment := new(Comment)
	if err := cc.Bind(comment); err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("bind comment error: %v", err),
		})
	}
	key := CommentKey(comment.Subjectid)
	host := cc.Request().Header.Get("X-Forwarded-For")
	guid := xid.New()
	uts := time.Now().Unix()
	cmds := []Command{}
	cmds = append(cmds, Command{
		Group: "LISTS",
		Key:   key,
		From:  "SELF",
	}, Command{
		Group: "HASHES",
		Key:   key,
		Field: CommentDetailKey(guid.String()),
		From:  "PREVIOUS_VALUE",
	}, Command{
		Group: "ZADD",
		Key:   key,
		From:  "VALUE",
		Value: guid.String(),
	})
	comment.Id = guid.String()
	comment.Host = host
	comment.Uts = uts
	comment.Redis = cmds
	jsonB, err := json.Marshal(comment)
	if err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("comment object marshal error: %v", err),
		})
	}
	native, _, err := cc.Codecs.Comment.NativeFromTextual(jsonB)
	if err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("convert json to native error: %v", err),
		})
	}
	binary, err := cc.Codecs.Comment.BinaryFromNative(nil, native)
	if err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("convert native to binary error: %v", err),
		})
	}
	msg := kafka.Message{
		Key:   []byte(key),
		Value: binary,
	}
	if err := produceMsg(&cc.Config, cc.Config.Comment.Topic, cc.Config.Comment.Ack, &msg); err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("produce msg error: %v", err),
		})
	}
	return cc.JSON(http.StatusOK, &SimpleResponse{Result: "success"})
}

type commentRure struct {
	Id     string
	Offset int64
	Codec  *goavro.Codec
}

// NewCommentRure ...
func NewCommentRure(id string, codec *goavro.Codec) commentRure {
	return commentRure{Id: id, Offset: -1, Codec: codec}
}

func (s *commentRure) Match(m kafka.Message) bool {
	native, _, err := s.Codec.NativeFromBinary(m.Value)
	if err != nil {
		return false
	}

	if v, ok := native.(map[string]interface{})["id"]; ok {
		if v.(string) == s.Id {
			s.Offset = m.Offset
		}
	}
	if s.Offset > 0 {
		return true
	}

	return false
}

// searchComment ...
func searchComment(c echo.Context) error {
	cc := c.(*CustomContext)
	lastid := cc.Param("subject_id")
	o := new([]Offset)
	if err := cc.Bind(o); err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("Bind error: %v", err),
		})
	}
	queue := make(chan kafka.Message)
	result := []string{}
	wg := &sync.WaitGroup{}
	for _, commentOffset := range *o {
		wg.Add(1)
		go func(conf Config, codec *goavro.Codec, topic string, partition int, offset int64, id string) {
			rule := NewCommentRure(id, codec)
			msgs := searchKafka(conf, codec, topic, partition, offset, &rule)
			for _, msg := range msgs {
				queue <- msg
			}
			wg.Done()
		}(cc.Config, cc.Codecs.Comment, commentOffset.Topic, int(commentOffset.Partition), commentOffset.Offset, lastid)
	}
	go func() {
		for q := range queue {
			result = append(result, string(q.Value))
		}
	}()
	wg.Wait()
	resp := []interface{}{}
	for _, binary := range result {
		native, _, err := cc.Codecs.Comment.NativeFromBinary([]byte(binary))
		if err != nil {
			return cc.JSON(http.StatusBadRequest, ErrResponse{
				Message: fmt.Sprintf("convert binary to native error: %v", err),
			})
		}
		resp = append(resp, native)
	}
	cc.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSONCharsetUTF8)
	cc.Response().WriteHeader(http.StatusOK)

	return json.NewEncoder(c.Response()).Encode(resp)
}
