package lib

import (
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/go-redis/redis"
	kafka "github.com/segmentio/kafka-go"
)

const OFFSET_KEY = "offset"

// Offset ...
func Offset(client *redis.Client, topic string, partition int) (int64, error) {
	field := fmt.Sprintf("%s:%d", topic, partition)
	result, err := client.HGet(OFFSET_KEY, field).Result()
	if err != nil {
		return -1, errors.New(fmt.Sprintf("read offset error: %v", err))
	}
	i64, err := strconv.ParseInt(result, 10, 64)
	if err != nil {
		return -1, errors.New(fmt.Sprintf("offset convert error: %v", err))
	}
	return i64, nil
}

// SetOffsetNX ...
func SetOffsetNX(client *redis.Client, topic string, partition int) error {
	field := fmt.Sprintf("%s:%d", topic, partition)
	if exists := client.HExists(OFFSET_KEY, field).Val(); !exists {
		if err := SetOffset(client, topic, partition, 0); err != nil {
			return err
		}
	}
	return nil
}

// SetOffset ...
func SetOffset(client *redis.Client, topic string, partition int, offset int64) error {
	value := strconv.FormatInt(offset, 10)
	field := fmt.Sprintf("%s:%d", topic, partition)
	if err := client.HSet(OFFSET_KEY, field, value).Err(); err != nil {
		return err
	}
	return nil
}

// ExecList ...
func ExecList(conf Config, client *redis.Client, cmd *Command, msg *kafka.Message, previousValue interface{}) (interface{}, error) {
	var result interface{}
	var err error
	if conf.Main.Debug {
		log.Printf("[RPush] key: %s\n", cmd.Key)
	}
	switch (*cmd).From {
	case "SELF":
		if conf.Main.Debug {
			log.Printf("[RPush] value(self): %s\n", msg.Value)
		}
		result, err = client.RPush(cmd.Key, msg.Value).Result()
		if err != nil {
			return result, err
		}
	case "PREVIOUS_VALUE":
		if conf.Main.Debug {
			log.Printf("[RPush] value(previous value): %s\n", previousValue)
		}
		result, err = client.RPush(cmd.Key, previousValue).Result()
		if err != nil {
			return result, err
		}
	case "VALUE":
		if conf.Main.Debug {
			log.Printf("[RPush] value(value): %s\n", cmd.Value)
		}
		result, err = client.RPush(cmd.Key, cmd.Value).Result()
		if err != nil {
			return result, err
		}
	default:
		log.Printf("[RPush] command no match: %s\n", cmd.From)
	}
	if conf.Main.Debug {
		log.Printf("[RPush] result: %v\n", result)
	}
	return result, nil
}

// ExecHash ...
func ExecHash(conf Config, client *redis.Client, cmd *Command, msg *kafka.Message, previousValue interface{}) (interface{}, error) {
	var result interface{}
	var err error
	if conf.Main.Debug {
		log.Printf("[HSet] key: %s, field: %s\n", cmd.Key, cmd.Field)
	}
	switch (*cmd).From {
	case "SELF":
		if conf.Main.Debug {
			log.Printf("[Hset] value(self): %s\n", msg.Value)
		}
		result, err = client.HSet(cmd.Key, cmd.Field, msg.Value).Result()
		if err != nil {
			return result, err
		}
	case "PREVIOUS_VALUE":
		if conf.Main.Debug {
			log.Printf("[Hset] value(previos value): %v\n", previousValue)
		}
		result, err = client.HSet(cmd.Key, cmd.Field, previousValue).Result()
		if err != nil {
			return result, err
		}
	case "VALUE":
		if conf.Main.Debug {
			log.Printf("[Hset] value(value): %s\n", cmd.Value)
		}
		result, err = client.HSet(cmd.Key, cmd.Field, cmd.Value).Result()
		if err != nil {
			return result, err
		}
	default:
		log.Printf("[Hset] command no match: %s\n", cmd.From)
	}
	if conf.Main.Debug {
		log.Printf("[Hset] result: %v\n", result)
	}
	return result, nil
}

// ExecSets ...
func ExecSets(conf Config, client *redis.Client, cmd *Command, msg *kafka.Message, previousValue interface{}) (interface{}, error) {
	var result interface{}
	var err error
	if conf.Main.Debug {
		log.Printf("[Set] key: %s\n", cmd.Key)
	}
	switch (*cmd).From {
	case "SELF":
		if conf.Main.Debug {
			log.Printf("[Set] value(self): %s\n", msg.Value)
		}
		result, err = client.Set(cmd.Key, msg.Value, 0).Result()
		if err != nil {
			return result, err
		}
	case "PREVIOUS_VALUE":
		if conf.Main.Debug {
			log.Printf("[Set] value(previos value): %v\n", previousValue)
		}
		result, err = client.Set(cmd.Key, previousValue, 0).Result()
		if err != nil {
			return result, err
		}
	case "VALUE":
		if conf.Main.Debug {
			log.Printf("[Set] value(value): %s\n", cmd.Value)
		}
		result, err = client.Set(cmd.Key, cmd.Value, 0).Result()
		if err != nil {
			return result, err
		}
	default:
		log.Printf("[Set] command no match: %s\n", cmd.From)
	}
	if conf.Main.Debug {
		log.Printf("[Set] result: %v\n", result)
	}
	return result, nil
}

// ExecuteLedisCmds ...
func ExecuteLedisCmds(conf Config, client *redis.Client, cmds *[]Command, msg *kafka.Message) error {
	var result interface{}
	var err error
	for _, cmd := range *cmds {
		if conf.Main.Debug {
			log.Printf("[ledisdb] cmd: %s\n", cmd.Group)
		}
		switch cmd.Group {
		case "LISTS":
			result, err = ExecList(conf, client, &cmd, msg, result)
			if err != nil {
				return err
			}
		case "HASHES":
			result, err = ExecHash(conf, client, &cmd, msg, result)
			if err != nil {
				return err
			}
		case "SETS":
			result, err = ExecSets(conf, client, &cmd, msg, result)
			if err != nil {
				return err
			}
		case "SORTEDSETS":
			if conf.Main.Debug {
				log.Printf("[ZIncrBy] key: %s, value(value):%s\n", cmd.Key, cmd.Value)
			}
			result, err = client.ZIncrBy(cmd.Key, 1, cmd.Value).Result()
			if err != nil {
				return err
			}
			if conf.Main.Debug {
				log.Printf("[ZIncrBy] result: %v\n", result)
			}
		}
	}
	return nil
}
