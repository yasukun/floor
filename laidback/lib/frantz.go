package lib

import (
	"context"
	"log"

	"github.com/go-redis/redis"
	"github.com/linkedin/goavro"
	kafka "github.com/segmentio/kafka-go"
)

type Command struct {
	Group string
	Key   string
	Field string
	From  string
	Value string
}

// ledisCmds ...
func ledisCmds(native *interface{}) []Command {
	// native data example:
	// map[redis:[map[value:test value group:LISTS key:sumbject:new field:] map[key:subject:new field:invert_idx:bcab4l2k2jbda3lsf41g value:1 group:HASHES]]]

	cmds := []Command{}
	var fields []interface{}
	if v, ok := (*native).(map[string]interface{})["redis"]; ok {
		fields = v.([]interface{})
	} else {
		// commad not exists
		return cmds
	}

	for _, field := range fields {

		cmds = append(cmds, Command{
			Group: field.(map[string]interface{})["group"].(string),
			Key:   field.(map[string]interface{})["key"].(string),
			Field: field.(map[string]interface{})["field"].(string),
			From:  field.(map[string]interface{})["from"].(string),
			Value: field.(map[string]interface{})["value"].(string),
		})
	}
	return cmds
}

// CommandExtraction ...
func CommandExtraction(codec *goavro.Codec, msg *kafka.Message) (cmds []Command, err error) {
	native, _, err := codec.NativeFromBinary(msg.Value)
	if err != nil {
		return
	}

	cmds = ledisCmds(&native)
	return
}

// ReadKafka ...
func ReadKafka(ctx context.Context, conf Config, client *redis.Client, codec *goavro.Codec, topic TopicConfig, partition int) error {
	brokers := []string{}
	for _, broker := range conf.Kafka.Brokers {
		brokers = append(brokers, broker.Addr)
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic.Topic,
		Partition: partition,
		MinBytes:  topic.Minbytes,
		MaxBytes:  topic.Maxbytes,
	})
	defer r.Close()

	offset, err := Offset(client, topic.Topic, partition)
	if err != nil {
		return err
	}
	r.SetOffset(offset)

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return err
		}

		if err = SetOffset(client, topic.Topic, partition, m.Offset+1); err != nil {
			log.Println("update offset error: ", err)
		}
		cmds, err := CommandExtraction(codec, &m)
		if err != nil {
			return err
		}

		if err = ExecuteLedisCmds(conf, client, &cmds, &m); err != nil {
			return err
		}
	}
	return nil
}
