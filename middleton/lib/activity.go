package lib

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo"
	"github.com/rs/xid"
	kafka "github.com/segmentio/kafka-go"
)

// favComment ...
func favComment(c echo.Context) error {
	cc := c.(*CustomContext)
	key := CommentKey(cc.Param("subject_id"))
	commentid := cc.Param("xid")
	a := new(Activity)
	if err := cc.Bind(a); err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("bind PostIDs error: %v", err),
		})
	}
	host := cc.Request().Header.Get("X-Forwarded-For")
	guid := xid.New()
	uts := time.Now().Unix()
	a.Id = guid.String()
	a.Host = host
	a.Uts = uts

	cmds := []Command{}
	cmds = append(cmds, Command{
		Group: "ZINCRBY",
		Key:   key,
		From:  "VALUE",
		Value: commentid,
	})
	jsonB, err := json.Marshal(a)
	if err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("activity object marshal error: %v", err),
		})
	}
	native, _, err := cc.Codecs.Activity.NativeFromTextual(jsonB)
	if err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("convert json to native error: %v", err),
		})
	}
	binary, err := cc.Codecs.Activity.BinaryFromNative(nil, native)
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

// subjectInc ...
func subjectInc(c echo.Context) error {
	cc := c.(*CustomContext)
	category := cc.Param("category")
	key := SubjectKey(category)
	val := cc.Param("xid")
	activity := new(Activity)
	if err := cc.Bind(activity); err != nil {
		return cc.JSON(http.StatusInternalServerError, ErrResponse{
			Message: fmt.Sprintf("Bind error: %v", err),
		})
	}

	guid := xid.New()
	uts := time.Now().Unix()
	host := cc.Request().Header.Get("X-Forwarded-For")
	cmds := []Command{}
	cmds = append(cmds, Command{
		Group: "ZINCRBY",
		Key:   key,
		From:  "VALUE",
		Value: val,
	})
	activity.Id = guid.String()
	activity.Uts = uts
	activity.Name = ""
	activity.Host = host
	activity.Redis = cmds

	jsonB, err := json.Marshal(activity)
	if err != nil {
		return cc.JSON(http.StatusInternalServerError, ErrResponse{
			Message: fmt.Sprintf("json marshal error: %v", err),
		})
	}
	native, _, err := cc.Codecs.Activity.NativeFromTextual(jsonB)
	if err != nil {
		return cc.JSON(http.StatusInternalServerError, ErrResponse{
			Message: fmt.Sprintf("convert textual to native error: %v", err),
		})
	}
	binary, err := cc.Codecs.Activity.BinaryFromNative(nil, native)
	if err != nil {
		return cc.JSON(http.StatusInternalServerError, ErrResponse{
			Message: fmt.Sprintf("convert native to binary error: %v", err),
		})
	}
	msg := kafka.Message{
		Key:   []byte(category),
		Value: binary,
	}
	if err := produceMsg(&cc.Config, cc.Config.Activity.Topic, cc.Config.Activity.Ack, &msg); err != nil {
		return cc.JSON(http.StatusInternalServerError, ErrResponse{
			Message: fmt.Sprintf("produce msg error: %v", err),
		})
	}
	success := &SimpleResponse{Result: "success"}
	return cc.JSON(http.StatusOK, success)
}

// favComments ...
func favComments(c echo.Context) error {
	cc := c.(*CustomContext)
	key := CommentKey(cc.Param("subject_id"))
	postRange := new(PostRange)
	if err := cc.Bind(postRange); err != nil {
		return cc.JSON(http.StatusInternalServerError, ErrResponse{
			Message: fmt.Sprintf("Bind error: %v", err),
		})
	}
	results, err := cc.Client.ZRangeWithScores(key, postRange.Start, postRange.Stop).Result()
	if err != nil {
		return cc.JSON(http.StatusInternalServerError, ErrResponse{
			Message: fmt.Sprintf("take comment rank data error: %v", err),
		})
	}
	resp := []Rank{}
	for _, result := range results {
		resp = append(resp, Rank{Score: result.Score, Member: result.Member})
	}
	return cc.JSON(http.StatusOK, &resp)
}
