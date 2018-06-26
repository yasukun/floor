package lib

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"github.com/labstack/echo"
)

// listMetainfo ...
func listMetainfo(c echo.Context) error {
	cc := c.(*CustomContext)
	metatype := cc.Param("type")
	locale := cc.Param("locale")
	k := ""
	switch metatype {
	case "categories":
		k = "categories"
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

	intermediate := map[string][]Metainfo{}
	for _, result := range results {
		native, _, err := cc.Codecs.Metainfo.NativeFromBinary([]byte(result))
		if err != nil {
			return cc.JSON(http.StatusBadRequest, ErrResponse{
				Message: fmt.Sprintf("convert binary to native error: %v", err),
			})
		}
		textual, err := cc.Codecs.Metainfo.TextualFromNative(nil, native)
		if err != nil {
			return cc.JSON(http.StatusBadRequest, ErrResponse{
				Message: fmt.Sprintf("convert native to textual error: %v", err),
			})
		}
		metainfo := new(Metainfo)
		if err := json.Unmarshal(textual, metainfo); err != nil {
			return cc.JSON(http.StatusBadRequest, ErrResponse{
				Message: fmt.Sprintf("textual unmarshal error: %v", err),
			})
		}
		index := ""
		for _, synonym := range metainfo.Synonyms {
			if locale == synonym.Locale {
				index = synonym.Index
				break
			}
		}
		intermediate[index] = append(intermediate[index], *metainfo)
	}

	resp := []MtIndexList{}

	ks := make([]string, 0, len(intermediate))
	for k := range intermediate {
		ks = append(ks, k)
	}

	sort.Strings(ks)

	for idx, k := range ks {
		if v, ok := intermediate[k]; ok {
			cells := []MtIndexCell{}
			for _, m := range v {
				for _, s := range m.Synonyms {
					cells = append(cells, MtIndexCell{Name: s.Name, CategoryID: m.ID})
				}
				mtIndexList := MtIndexList{
					Index: k,
					Order: idx,
					Cells: cells,
				}
				resp = append(resp, mtIndexList)
			}
		}
	}

	cc.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSONCharsetUTF8)
	cc.Response().WriteHeader(http.StatusOK)
	return json.NewEncoder(c.Response()).Encode(resp)
}
