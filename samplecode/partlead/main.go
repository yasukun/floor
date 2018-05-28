package main

import (
	"context"
	"fmt"
	"log"
	"time"

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
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "roure.avro.post", 1)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	conn.Seek(1000, kafka.SeekEnd)
	batch := conn.ReadBatch(10e3, 1e6)
	b := make([]byte, 10e3)
	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		native, _, err := codec.NativeFromBinary(b)
		if err != nil {
			log.Fatalln("NativeFromBinary error: ", err)
		}
		textual, err := codec.TextualFromNative(nil, native)
		if err != nil {
			log.Fatalln("TextualFromNative error: ", err)
		}
		fmt.Println(string(textual))
	}
	fmt.Println("offset: ", batch.Offset())
	batch.Close()
	conn.Close()
}
