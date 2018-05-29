package lib

import (
	"context"
	"errors"
	"log"

	"time"

	"github.com/go-redis/redis"
	"github.com/linkedin/goavro"
	kafka "github.com/segmentio/kafka-go"
)

// decodeMsg ...
func decodeMsg(codec *goavro.Codec, msg kafka.Message) (redisKey, redisCommand, body string, err error) {

	native, _, err := codec.NativeFromBinary(msg.Value)
	if err != nil {
		return
	}

	if v, ok := native.(map[string]interface{})["redis_key"]; ok {
		redisKey = v.(string)
	} else {
		err = errors.New("key error [redis_key]")
		return
	}
	if v, ok := native.(map[string]interface{})["redis"]; ok {
		redisCommand = v.(string)
	} else {
		err = errors.New("key error [redis]")
		return
	}

	textual, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return
	}
	body = string(textual)
	return
}

// ReadKafka ...
func ReadKafka(conf Config, client *redis.Client, codec *goavro.Codec, partition int) error {
	brokers := []string{}
	for _, broker := range conf.Kafka.Brokers {
		brokers = append(brokers, broker.Addr)
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     conf.Kafka.Topic,
		Partition: partition,
		MinBytes:  conf.Kafka.Minbytes,
		MaxBytes:  conf.Kafka.Maxbytes,
	})
	defer r.Close()

	offset, err := Offset(client, conf.Kafka.Topic, partition)
	if err != nil {
		return err
	}
	r.SetOffset(offset)

	ch := make(chan int64)
	go func(conf Config, client *redis.Client, partition int, offset int64) {
		curoffset := offset
		for {
			select {
			case v := <-ch:
				curoffset = v
			default:
				if err = SetOffset(client, conf.Kafka.Topic, partition, curoffset); err != nil {
					log.Println("Set offset error: ", err)
				}
				time.Sleep(time.Duration(conf.Main.UpdateOffsetWait) * time.Second)
			}
		}
	}(conf, client, partition, offset)
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			return err
		}
		redisKey, redisCmd, body, err := decodeMsg(codec, m)
		if err != nil {
			return err
		}

		if err = storeMessage(client, redisCmd, redisKey, body); err != nil {
			return err
		}
	}
}
