package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"os/signal"

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

type Codecs struct {
	Conf lib.Config
}

// (c Codecs) Get ...
func (c Codecs) Get(topicName string) (*goavro.Codec, error) {
	var codec *goavro.Codec
	for _, topic := range c.Conf.Kafka.Topics {
		if topic.Topic == topicName {
			schema, err := Asset(topic.AvroSchema)
			if err != nil {
				return codec, err
			}
			codec, err = goavro.NewCodec(string(schema))
			if err != nil {
				return codec, err
			}
			return codec, nil
		}
	}

	return codec, fmt.Errorf("schema(%s) not found", topicName)
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

	codecs := Codecs{Conf: conf}

	client := redis.NewClient(&redis.Options{
		Addr:     conf.Ledisdb.Addr,
		Password: conf.Ledisdb.Password,
		DB:       conf.Ledisdb.DB,
	})

	pong, err := client.Ping().Result()
	if err != nil {
		log.Fatalln("ledisdb ping: ", err)
	}
	log.Printf("ledisdb ping: %s", pong)
	log.Println("laidback start")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	for _, topic := range conf.Kafka.Topics {
		for i := 0; i < topic.Partitions; i++ {
			if err := lib.SetOffsetNX(client, topic.Topic, i); err != nil {
				log.Printf("set offset if not exist error (topic=%s): %v\n", topic.Topic, err)
			}
			go func(ctx context.Context, conf lib.Config, client *redis.Client, topic lib.TopicConfig, partition int) {
				log.Printf("  spawn listener (topic: %s partition: %d)\n", topic.Topic, partition)
				codec, err := codecs.Get(topic.Topic)
				if err != nil {
					log.Fatalln("take codec error: ", err)
				}
				if err := lib.ReadKafka(ctx, conf, client, codec, topic, partition); err != nil {
					log.Printf("kafka read error: %v\n", err)
				}
			}(ctx, conf, client, topic, i)
		}
	}
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	cancel()
	select {
	case <-ctx.Done():
		log.Println("laidback stop:", ctx.Err())
	}

	os.Exit(0)
}
