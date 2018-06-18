package main

import (
	"log"
	"os"
	"strconv"

	"encoding/csv"
	"encoding/json"

	"github.com/BurntSushi/toml"
	"github.com/go-redis/redis"
	"github.com/linkedin/goavro"
)

type Synonym struct {
	Name   string `json:"name"`
	Locale string `json:"locales"`
}

type Metainfo struct {
	ID       string    `json:"id"`
	Synonyms []Synonym `json:"synonyms"`
}

type Metatuple struct {
	Score    float64
	Metainfo Metainfo
}

type Config struct {
	Metainfo MetaConfig  `toml:"metainfo"`
	Ledisdb  LedisConfig `toml:"ledisdb"`
}

type MetaConfig struct {
	Categories string `toml:"categories"`
	Tags       string `toml:"tags"`
	Schema     string `toml:"schema"`
}

type LedisConfig struct {
	Addr     string `toml:"addr"`
	Password string `toml:"password"`
	DB       int    `toml:"db"`
}

var config Config

// Init ...
func Init(confpath string) (err error) {
	var c Config
	if _, err = toml.DecodeFile(confpath, &c); err != nil {
		log.Printf("[metainfo plugin] init error: %v\n", err)
		return
	}
	config = c
	for _, filename := range []string{config.Metainfo.Categories, config.Metainfo.Tags} {
		if _, err = os.Stat(filename); err != nil {
			log.Printf("[metainfo plugin] %s does not exists: %v\n", filename, err)
			return
		}
	}
	return
}

// readMetainfo ...
func readMetainfo(metafile string) (metatuple []Metatuple, err error) {
	file, err := os.Open(metafile)
	if err != nil {
		log.Printf("[metainfo plugin] file open error: %v\n", err)
		return
	}
	defer file.Close()
	r := csv.NewReader(file)
	records, err := r.ReadAll()
	if err != nil {
		log.Printf("[metainfo plugin] csv read all error: %v\n", err)
		return
	}
	metatuple = []Metatuple{}
	for _, record := range records {
		category := record[0]
		name := record[1]
		score := record[2]
		var f64 float64
		f64, err = strconv.ParseFloat(score, 64)
		if err != nil {
			log.Printf("[metainfo plugin] score (%s) parse float error: %v\n", score, err)
			return
		}
		synonym := Synonym{Name: name, Locale: "ja"}
		metatuple = append(metatuple, Metatuple{Score: f64, Metainfo: Metainfo{ID: category, Synonyms: []Synonym{synonym}}})
	}
	return
}

// Execute ...
func Execute() (err error) {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Ledisdb.Addr,
		Password: config.Ledisdb.Password,
		DB:       config.Ledisdb.DB,
	})

	schema, err := Asset(config.Metainfo.Schema)
	if err != nil {
		log.Printf("[metainfo plugin] load asset error: %v\n", err)
		return err
	}
	codec, err := goavro.NewCodec(string(schema))
	if err != nil {
		log.Printf("[metainfo plugin] create codec error: %v\n", err)
		return err
	}

	for _, keyname := range []string{"categories", "tags"} {
		var metatuple []Metatuple
		switch keyname {
		case "categories":
			metatuple, err = readMetainfo(config.Metainfo.Categories)
		case "tags":
			metatuple, err = readMetainfo(config.Metainfo.Tags)
		default:
			continue
		}
		for _, meta := range metatuple {
			jsonB, err := json.Marshal(meta.Metainfo)
			if err != nil {
				log.Printf("[metainfo plugin] json marshal error: %v\n", err)
				return err
			}
			native, _, err := codec.NativeFromTextual(jsonB)
			if err != nil {
				log.Printf("[metainfo plugin] convert json to native error: %v\n", err)
				return err
			}
			binary, err := codec.BinaryFromNative(nil, native)
			if err != nil {
				log.Printf("[metainfo plugin] convert native to binary error: %v\n", err)
				return err
			}
			if err := client.ZAdd(keyname, redis.Z{Score: meta.Score, Member: binary}).Err(); err != nil {
				log.Printf("[metainfo plugin] zadd error: %v\n", err)
				return err
			}
		}
	}
	return nil
}

func main() {
	log.Println("Init")
	if err := Init("../steve.toml"); err != nil {
		log.Fatalln(err)
	}
	log.Println("Execute")
	if err := Execute(); err != nil {
		log.Fatalln(err)
	}
	log.Println("done.")
}
