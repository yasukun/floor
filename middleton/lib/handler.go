package lib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"strconv"

	"github.com/go-redis/redis"
	"github.com/labstack/echo"
	"github.com/linkedin/goavro"
)

type Codecs struct {
	Subject *goavro.Codec
}

type CustomContext struct {
	echo.Context
	Config
	*redis.Client
	Codecs
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
		return errors.New(fmt.Sprintf("decode detail subject error: %v", err))
	}
	return cc.JSON(http.StatusOK, native)
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
	cc.Logger().Infof("bind: %v", s)
	category := cc.Param("category")

	cc.Logger().Infof("category: %s", category)
	req := cc.Request()
	host := req.Header.Get("X-Forwarded-For")

	msg, resp, err := newSubjectMsg(cc.Codecs.Subject, category, host, s)
	if err != nil {
		cc.Logger().Errorf("create subject msg error: %v", err)
		return err
	}

	w := subjectWriter(cc.Config)
	defer w.Close()
	if err := w.WriteMessages(context.Background(), msg); err != nil {
		cc.Logger().Errorf("write kafka error: %v", err)
		return err
	}
	return cc.JSON(http.StatusOK, resp)

}

// Routes ...
func Routes(e *echo.Echo) {
	r := e.Group("/api")
	r.GET("/latest/subject/:category", latestSubject)
	r.GET("/subject/:category/:xid", detailSubject)
	r.POST("/subject/:category", newSubject)
}
