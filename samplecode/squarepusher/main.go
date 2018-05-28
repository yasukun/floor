package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"

	"time"

	"encoding/json"

	"github.com/rs/xid"
	kafka "github.com/segmentio/kafka-go"
	goavro "gopkg.in/linkedin/goavro.v2"
)

type Tag struct {
	Name string `json:"name"`
}

type Image struct {
	Src string `json:"src"`
}

type PostData struct {
	Xid         string  `json:"xid"`
	Topic       string  `json:"topic"`
	Name        string  `json:"name"`
	Uts         int64   `json:"uts"`
	Host        string  `json:"host"`
	FingerPrint string  `json:"fingerprint"`
	Subject     string  `json:"subject"`
	Url         string  `json:"url"`
	Tags        []Tag   `json:"tags"`
	Images      []Image `json:"images"`
}

var addr = flag.String("a", "localhost:9092", "kafka address")
var topic = flag.String("t", "roure.avro.post", "kafka topic")
var partition = flag.Int("p", 1, "topic partition")

func main() {
	flag.Parse()
	postscheme, err := Asset("roure.avro/post.avsc")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(postscheme))
	codec, err := goavro.NewCodec(string(postscheme))
	if err != nil {
		log.Fatalln(err)
	}

	rand.Seed(time.Now().UnixNano())
	keys := []string{"news", "program", "music"}
	fingers := []string{"finger1", "finger2", "finger3"}
	host := []string{"host1", "host2", "host3"}
	subjects := []string{"subjects1", "subjects2", "subjects3"}
	tags := []Tag{}
	tags = append(tags, Tag{Name: "kenmo"})
	tags = append(tags, Tag{Name: "zatsudan"})
	images := []Image{}
	conn, err := kafka.DialContext(context.Background(), "tcp", *addr)
	// conn, err := kafka.DialLeader(context.Background(), "tcp", *addr, *topic, *partition)
	// if err != nil {
	// 	log.Fatalln("DialLeader error: ", err)
	// }

	topicConfig := kafka.TopicConfig{Topic: *topic, NumPartitions: *partition, ReplicationFactor: 1}
	if err = conn.CreateTopics(topicConfig); err != nil {
		log.Fatalln("createTopics error: ", err)
	}
	conn.Close()

	// conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{*addr},
		Topic:        *topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: 0,
	})
	ctx := context.Background()
	msgs := []kafka.Message{}
	for i := 0; i < 100000; i++ {

		guid := xid.New()
		uts := time.Now().Unix()
		r := rand.Intn(3)
		p := PostData{
			Xid:         guid.String(),
			Topic:       keys[r],
			Name:        "774",
			Uts:         uts,
			Host:        host[r],
			FingerPrint: fingers[r],
			Subject:     subjects[r],
			Url:         "http://google.co.jp",
			Tags:        tags,
			Images:      images,
		}
		jsonB, err := json.Marshal(p)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println(string(jsonB))
		native, _, err := codec.NativeFromTextual(jsonB)
		if err != nil {
			log.Fatalln(err)
		}
		binary, err := codec.BinaryFromNative(nil, native)
		if err != nil {
			log.Fatalln(err)
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(keys[r]),
			Value: binary,
		})
		// w.WriteMessages(ctx, kafka.Message{
		// 	Key:   []byte(keys[r]),
		// 	Value: binary,
		// })
		// w.Close()
	}
	w.WriteMessages(ctx, msgs...)
	w.Close()
}
