package lib

import (
	"github.com/go-redis/redis"
	"github.com/labstack/echo"
	"github.com/linkedin/goavro"
)

type Codecs struct {
	Subject  *goavro.Codec
	Comment  *goavro.Codec
	Activity *goavro.Codec
	Metainfo *goavro.Codec
}

type CustomContext struct {
	echo.Context
	Config
	*redis.Client
	Codecs
}

// llen ...
func llen(client *redis.Client, key string) (i64 int64, err error) {
	i64, err = client.LLen(key).Result()
	return
}

// Routes ...
func Routes(e *echo.Echo) {
	r := e.Group("/api")

	// category & tag
	r.GET("/meta/:type/list", listMetainfo)

	// subject
	r.GET("/subject/len/:category", lenSubject)
	r.GET("/subject/detail/:category/:xid", detailSubject)
	r.GET("/subject/latest/:category", latestSubject)
	r.GET("/subject/index/:category/:xid", indexSubject)
	r.GET("/subject/len/:subject_id", lenSubject)
	r.POST("/subject/new/:category", newSubject)
	r.POST("/subject/range/:category", rangeSubject)
	r.POST("/subject/search/:category/:xid", searchSubject)

	// kafka
	r.GET("/offset/:filter", searchOffset)

	// comment
	r.POST("/comment/detail_byids/:subject_id", detailComments)
	r.POST("/comment/new/", newComment)
	r.POST("/comment/range/:subject_id", rangeComment)
	r.POST("/comment/search/:subject_id", rangeComment)

	// activity
	r.POST("/activity/favarite/comment/:subject_id/:xid", favComment)
	r.POST("/activity/inc/view/subject/:category/:xid", subjectInc)
  r.POST("/activity/favarite/comments/:subject_id", favComments)
}
