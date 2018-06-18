package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/linkedin/goavro"
	"github.com/rs/xid"
	kafka "github.com/segmentio/kafka-go"
)

type Config struct {
	SampleSubject SampleSubjectConfig `toml:"samplesubject"`
	Kafka         KafkaConfig         `toml:"kafka"`
}

type KafkaConfig struct {
	Brokers []Broker `toml:"broker"`
	Topic   string   `toml:"topic"`
	Ack     int      `toml:"ack"`
	Timeout int      `toml:"timeout"`
}

type Broker struct {
	Addr string `toml:"addr"`
}

type SampleSubjectConfig struct {
	Schema string `toml:"schema"`
	Debug  bool   `toml:"debug"`
}

type Command struct {
	Group string `json:"group"`
	Key   string `json:"key"`
	Field string `json:"field"`
	From  string `json:"from"`
	Value string `json:"value"`
}

type Tag struct {
	Name string `json:"name"`
}

type Image struct {
	Src string `json:"src"`
}

type Og struct {
	Url         string `json:"url"`
	Type        string `json:"type"`
	Image       string `json:"image"`
	Description string `json:"description"`
	Determiner  string `json:"determiner"`
	Sitename    string `json:"sitename"`
	Video       string `json:"video"`
}

type Subject struct {
	Id          string    `json:"id"`
	Category    string    `json:"category"`
	Name        string    `json:"name"`
	Uts         int64     `json:"uts"`
	Host        string    `json:"host"`
	FingerPrint string    `json:"fingerprint"`
	Body        string    `json:"body"`
	Opengraph   Og        `json:"opengraph"`
	Redis       []Command `json:"redis"`
	Tags        []Tag     `json:"tags"`
	Images      []Image   `json:"images"`
}

var config Config
var codec *goavro.Codec

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Init ...
func Init(configpath string) (err error) {
	var c Config
	if _, err = toml.DecodeFile(configpath, &c); err != nil {
		log.Printf("[sample subject plugin] init error: %v\n", err)
		return
	}
	config = c
	schema, err := Asset(config.SampleSubject.Schema)
	if err != nil {
		log.Printf("[sample subject] load asset error: %v\n", err)
		return err
	}
	codec, err = goavro.NewCodec(string(schema))
	if err != nil {
		log.Printf("[sample subject] create codec error: %v\n", err)
		return err
	}
	return
}

// createSubject ...
func createSubject() (subject Subject) {
	guid := xid.New()
	uts := time.Now().Unix()
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
		From:  "PREVIOUS_VALUE",
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

	msgs := []string{"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.", "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.", "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."}
	categories := []string{"news", "program", "live"}
	names := []string{"774", "", "chinneki"}
	r := rand.Intn(3)
	subject = Subject{
		Id:          guid.String(),
		Category:    categories[r],
		Name:        names[r],
		Uts:         uts,
		Host:        "127.0.0.1",
		FingerPrint: "12345",
		Body:        msgs[r],
		Opengraph:   og,
		Redis:       cmds,
		Tags:        tags,
		Images:      imgs,
	}
	return
}

// seriSubject ...
func seriSubject(subject *Subject) (binary []byte, err error) {

	jsonB, err := json.Marshal(*subject)
	if err != nil {
		log.Printf("[sample subject] json marshal error: %v\n", err)
		return
	}
	if config.SampleSubject.Debug {
		fmt.Println(string(jsonB))
	}
	native, _, err := codec.NativeFromTextual(jsonB)
	if err != nil {
		log.Printf("[sample subject] convert json to native error: %v\n", err)
		return
	}
	binary, err = codec.BinaryFromNative(nil, native)
	if err != nil {
		log.Printf("[sample subject] convert native to binary error: %v\n", err)
		return
	}
	return
}

// Execute ...
func Execute() (err error) {
	brokers := []string{}
	for _, broker := range config.Kafka.Brokers {
		brokers = append(brokers, broker.Addr)
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        config.Kafka.Topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: config.Kafka.Ack,
		WriteTimeout: time.Duration(config.Kafka.Timeout) * time.Second,
	})
	defer w.Close()
	ctx := context.Background()
	subject := createSubject()
	binary, err := seriSubject(&subject)
	if err != nil {
		log.Printf("[sample subject] serialize error: %v\n", err)
		return err
	}
	msg := kafka.Message{
		Key:   []byte(subject.Category),
		Value: binary,
	}
	w.WriteMessages(ctx, msg)
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
