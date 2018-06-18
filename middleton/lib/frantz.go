package lib

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo"
	"github.com/linkedin/goavro"
	kafka "github.com/segmentio/kafka-go"
)

// searchOffset ...
func searchOffset(c echo.Context) error {
	cc := c.(*CustomContext)
	filter := cc.Param("filter")
	alloffset, err := cc.Client.HGetAll("offset").Result()
	if err != nil {
		cc.Logger().Errorf("redisdb hgetall error: %v", err)
		return err
	}
	offsets := []Offset{}
	for k, v := range alloffset {
		fields := strings.Split(k, ":")
		partition, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			cc.Logger().Errorf("partiton convert error: %v", err)
			return err
		}
		offset, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			cc.Logger().Errorf("offset convert error: %v", err)
			return err
		}
		offsets = append(offsets, Offset{
			Topic:     fields[0],
			Partition: partition,
			Offset:    offset,
		})
	}
	filterd := []Offset{}
	switch filter {
	case "subject":
		for _, offset := range offsets {
			if offset.Topic == cc.Config.Subject.Topic {
				filterd = append(filterd, offset)
			}
		}
	default:
		filterd = offsets
	}
	return cc.JSON(http.StatusOK, filterd)
}

// produceMsg ...
func produceMsg(conf *Config, topic string, ack int, msg *kafka.Message) error {
	addrs := []string{}
	for _, broker := range conf.Kafka.Brokers {
		addrs = append(addrs, broker.Addr)
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      addrs,
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: ack,
		WriteTimeout: time.Duration(conf.Kafka.WriteTimeout) * time.Second,
	})
	defer w.Close()
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(conf.Kafka.WriteTimeout)*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, *msg); err != nil {
		return err
	}

	return nil
}

// searchKafka ...
func searchKafka(conf Config, codec *goavro.Codec, topic string, partition int, offset int64, rule Filter) []kafka.Message {
	brokers := []string{}
	for _, broker := range conf.Kafka.Brokers {
		brokers = append(brokers, broker.Addr)
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: partition,
		MinBytes:  conf.Kafka.Minbytes,
		MaxBytes:  conf.Kafka.Maxbytes,
		MaxWait:   time.Duration(conf.Kafka.MaxWait) * time.Second,
	})
	defer r.Close()
	r.SetOffset(offset - 1)
	msgs := []kafka.Message{}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(conf.Kafka.Cancel)*time.Second)
	defer cancel()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}
		if rule.Match(m) {
			msgs = append(msgs, m)
		}
	}
	return msgs
}

type Filter interface {
	Match(m kafka.Message) bool
}
