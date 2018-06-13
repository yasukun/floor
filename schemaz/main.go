// avro schema test and example
//
// First we will create an embedded file in the roure.avro directory
//
// ```
// $ go-bindata roure.avro/
// ```
//
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"

	"time"

	"github.com/go-redis/redis"
	"github.com/rs/xid"
	kafka "github.com/segmentio/kafka-go"
	goavro "gopkg.in/linkedin/goavro.v2"
)

// NewSubject ...
func NewSubject() Subject {
	guid := xid.New()

	cmds := []Command{}
	cmds = append(cmds, Command{
		Group: "LISTS",
		Key:   "sumbject:new",
		From:  "SELF",
		Value: "test value",
	})
	cmds = append(cmds, Command{
		Group: "HASHES",
		Key:   "subject:new",
		Field: "invert_idx:" + guid.String(),
		From:  "PREVINT",
		Value: "1",
	})

	tags := []Tag{}
	tags = append(tags, Tag{Name: "kenmo"})
	imgs := []Image{}
	imgs = append(imgs, Image{Src: "http://imgur.com/test.jpg"})

	og := Og{
		Url:         "http://newssite.com",
		Type:        "image.jpeg",
		Description: "description",
		Sitename:    "sitename",
	}

	uts := time.Now().Unix()
	subject := Subject{
		Id:          guid.String(),
		Category:    "news",
		Name:        "774",
		Uts:         uts,
		Host:        "127.0.0.1",
		FingerPrint: "12345",
		Body:        "main message",
		Opengraph:   og,
		Redis:       cmds,
		Tags:        tags,
		Images:      imgs,
	}
	return subject
}

// SubjectSample ...
func SubjectSample() ([]byte, error) {
	var binary []byte
	schema, err := Asset("roure.avro/subject.avsc")
	if err != nil {
		return binary, err
	}
	codec, err := goavro.NewCodec(string(schema))
	if err != nil {
		return binary, err
	}
	source, err := json.Marshal(NewSubject())
	if err != nil {
		return binary, err
	}
	native, _, err := codec.NativeFromTextual(source)
	if err != nil {
		return binary, err
	}
	binary, err = codec.BinaryFromNative(nil, native)
	if err != nil {
		return binary, err
	}
	native, _, err = codec.NativeFromBinary(binary)
	if err != nil {
		return binary, err
	}
	textual, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return binary, err
	}
	fmt.Println(string(textual))
	return binary, nil
}

// SubjectLedisSample ...
func SubjectLedisSample(binary *[]byte) error {

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6380",
		Password: "",
		DB:       0,
	})
	if err := client.RPush("sample1subject:news", *binary).Err(); err != nil {
		log.Fatalln("RPUSH error: ", err)
	}
	lists, err := client.LRange("sample1subject:news", 0, -1).Result()
	if err != nil {
		log.Fatalln("LRANGE error: ", err)
	}
	schema, err := Asset("roure.avro/subject.avsc")
	if err != nil {
		return err
	}
	codec, err := goavro.NewCodec(string(schema))
	if err != nil {
		return err
	}
	for _, v := range lists {
		native, _, err := codec.NativeFromBinary([]byte(v))
		if err != nil {
			return err
		}
		fmt.Println("-- native --")
		fmt.Println(native)
		textual, err := codec.TextualFromNative(nil, native)
		if err != nil {
			return err
		}
		fmt.Println("-- textual --")
		fmt.Println(string(textual))
	}
	return nil
}

// Usage ...
func Usage() {
	fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], ":\n")
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, "\n")
}

// NewTestSubjectData ...
func NewTestSubjectData(category string, target int) Subject {
	guid := xid.New()
	uts := time.Now().Unix()
	keyFormat := "subject:%s"

	msgs := []string{"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.", "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.", "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."}
	cmds := []Command{}
	cmds = append(cmds, Command{
		Group: "LISTS",
		Key:   fmt.Sprintf(keyFormat, category),
		From:  "SELF",
		Value: "test value",
	})
	cmds = append(cmds, Command{
		Group: "HASHES",
		Key:   fmt.Sprintf(keyFormat, category),
		Field: "Inverted:" + guid.String(),
		From:  "PREVIOUS_VALUE",
		Value: "",
	})
	cmds = append(cmds, Command{
		Group: "ZADD",
		Key:   fmt.Sprintf(keyFormat, category),
		From:  "VALUE",
		Value: guid.String(),
	})
	tags := []Tag{}
	tags = append(tags, Tag{Name: "zatsudan"})
	imgs := []Image{}
	imgs = append(imgs, Image{Src: "http://imgur.com/test.jpg"})
	og := Og{
		Url:         "http://newssite.com",
		Type:        "image.jpeg",
		Description: "description",
		Sitename:    "sitename",
	}
	subject := Subject{
		Id:          guid.String(),
		Category:    category,
		Name:        "774",
		Uts:         uts,
		Host:        "127.0.0.1",
		FingerPrint: "12345",
		Body:        msgs[target],
		Opengraph:   og,
		Redis:       cmds,
		Tags:        tags,
		Images:      imgs,
	}
	return subject
}

// WriteSubject ...
func WriteSubject(addr string, size int) error {
	schema, err := Asset("roure.avro/subject.avsc")
	if err != nil {
		return err
	}
	codec, err := goavro.NewCodec(string(schema))
	if err != nil {
		return err
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{addr},
		Topic:        "roure.avro.subject",
		Balancer:     &kafka.Hash{},
		RequiredAcks: 0,
	})

	ctx := context.Background()
	msgs := []kafka.Message{}
	rand.Seed(time.Now().UnixNano())
	categorys := []string{"news", "program", "music"}
	for i := 0; i < size; i++ {
		r := rand.Intn(3)
		s := NewTestSubjectData(categorys[r], r)
		jsonB, err := json.Marshal(s)
		if err != nil {
			return err
		}
		fmt.Println(string(jsonB))
		native, _, err := codec.NativeFromTextual(jsonB)
		if err != nil {
			return err
		}
		binary, err := codec.BinaryFromNative(nil, native)
		if err != nil {
			return err
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(categorys[r]),
			Value: binary,
		})
	}
	w.WriteMessages(ctx, msgs...)
	w.Close()
	return nil
}

func main() {
	flag.Usage = Usage
	flush := flag.Bool("flush", false, "flush all data")
	subject := flag.Bool("subject", false, "insert subject")
	kafka := flag.String("kafka", "localhost:9092", "kafka addr")
	ledis := flag.String("ledis", "localhost:6380", "ledisdb addr")
	datasize := flag.Int("size", 100, "size of test data")
	flag.Parse()

	client := redis.NewClient(&redis.Options{
		Addr:     *ledis,
		Password: "",
		DB:       0,
	})

	if *flush {
		log.Println("flush db")
		client.FlushAll()
		os.Exit(0)
	}

	if *subject {
		log.Println("write test data")
		if err := WriteSubject(*kafka, *datasize); err != nil {
			log.Fatalln(err)
		}
		os.Exit(0)
	}

	// log.Println("avro test")
	// binary, err := SubjectSample()
	// if err != nil {
	// 	log.Fatalln("subject error: ", err)
	// }
	// if err := SubjectLedisSample(&binary); err != nil {
	// 	log.Fatalln("push ledisdb error: ", err)
	// }
	Usage()
}
