package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/go-redis/redis"
	"github.com/linkedin/goavro"
	"github.com/yasukun/roure/laidback/lib"
)

// Usage ...
func Usage() {
	fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], ":\n")
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, "\n")
}

// setCodec ...
func setCodec(conf lib.Config) (*goavro.Codec, error) {
	var codec *goavro.Codec
	schema, err := Asset(conf.Avro.Schema)
	if err != nil {
		return codec, err
	}
	codec, err = goavro.NewCodec(string(schema))
	if err != nil {
		return codec, err
	}
	return codec, nil
}

func main() {
	flag.Usage = Usage
	confname := flag.String("c", "laidback.toml", "path to config")
	flag.Parse()

	_, err := os.Stat(*confname)
	if err != nil {
		log.Fatalln("config file error: ", err)
	}

	conf, err := lib.DecodeConfigToml(*confname)
	if err != nil {
		log.Fatalln("decode config error", err)
	}

	codec, err := setCodec(conf)
	if err != nil {
		log.Fatalln("avro codec error: ", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr:     conf.Ledisdb.Addr,
		Password: conf.Ledisdb.Password,
		DB:       conf.Ledisdb.DB,
	})

	wg := &sync.WaitGroup{}
	for i := 0; i < conf.Kafka.Partitions; i++ {
		wg.Add(1)
		go func(conf lib.Config, client *redis.Client, partition int) {
			err := lib.ReadKafka(conf, client, codec, partition)
			if err != nil {
				log.Println("kafka read error:", err)
			}
			wg.Done()
		}(conf, client, i)
	}
	wg.Wait()
	os.Exit(0)
}
