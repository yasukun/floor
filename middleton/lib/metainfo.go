package lib

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo"
)

// listMetainfo ...
func listMetainfo(c echo.Context) error {
	cc := c.(*CustomContext)
	metatype := cc.Param("type")
	k := ""
	switch metatype {
	case "categories":
		k = "categoies"
	case "tags":
		k = "tags"
	default:
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("%s mismatch type", metatype),
		})
	}
	results, err := cc.Client.ZRevRange(k, 0, -1).Result()
	if err != nil {
		return cc.JSON(http.StatusBadRequest, ErrResponse{
			Message: fmt.Sprintf("zrevrange error: %v", err),
		})
	}

	resp := []interface{}{}
	for _, result := range results {
		native, _, err := cc.Codecs.Metainfo.NativeFromBinary([]byte(result))
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
