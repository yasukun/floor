package main

import (
	"context"
	"fmt"
	"log"

	"github.com/linkedin/goavro"
	kafka "github.com/segmentio/kafka-go"
)

func main() {

	postscheme, err := Asset("roure.avro/post.avsc")
	if err != nil {
		log.Fatalln("Asset error: ", err)
	}
	codec, err := goavro.NewCodec(string(postscheme))
	if err != nil {
		log.Fatalln("NewCodec error: ", err)
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "consumer-group-id",
		Topic:    "roure.avro.post",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}

		native, _, err := codec.NativeFromBinary(m.Value)
		if err != nil {
			log.Fatalln("NativeFromBinary error: ", err)
		}
		textual, err := codec.TextualFromNative(nil, native)
		if err != nil {
			log.Fatalln("TextualFromNative error: ", err)
		}
		fmt.Println(string(textual))
	}
	r.Close()
}
