package lib

import (
	"errors"
	"fmt"
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
func ExecList(client *redis.Client, cmd *Command, msg *kafka.Message, previousValue interface{}) (interface{}, error) {
	var result interface{}
	var err error
	switch (*cmd).From {
	case "SELF":
		result, err = client.RPush(cmd.Key, msg.Value).Result()
		if err != nil {
			return result, err
		}
	case "PREVIOS_VALUE":
		result, err = client.RPush(cmd.Key, previousValue).Result()
		if err != nil {
			return result, err
		}
	case "VALUE":
		result, err = client.RPush(cmd.Key, cmd.Value).Result()
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// ExecHash ...
func ExecHash(client *redis.Client, cmd *Command, msg *kafka.Message, previousValue interface{}) (interface{}, error) {
	var result interface{}
	var err error
	switch (*cmd).From {
	case "SELF":
		result, err = client.HSet(cmd.Key, cmd.Field, msg.Value).Result()
		if err != nil {
			return result, err
		}
	case "PREVIOS_VALUE":
		result, err = client.HSet(cmd.Key, cmd.Field, previousValue).Result()
		if err != nil {
			return result, err
		}
	case "VALUE":
		result, err = client.HSet(cmd.Key, cmd.Field, cmd.Value).Result()
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// ExecSets ...
func ExecSets(client *redis.Client, cmd *Command, msg *kafka.Message, previousValue interface{}) (interface{}, error) {
	var result interface{}
	var err error
	switch (*cmd).From {
	case "SELF":
		result, err = client.Set(cmd.Key, msg.Value, 0).Result()
		if err != nil {
			return result, err
		}
	case "PREVIOS_VALUE":
		result, err = client.Set(cmd.Key, previousValue, 0).Result()
		if err != nil {
			return result, err
		}
	case "VALUE":
		result, err = client.Set(cmd.Key, cmd.Value, 0).Result()
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// ExecuteLedisCmds ...
func ExecuteLedisCmds(client *redis.Client, cmds *[]Command, msg *kafka.Message) error {
	var result interface{}
	var err error
	for _, cmd := range *cmds {
		switch cmd.Group {
		case "LISTS":
			result, err = ExecList(client, &cmd, msg, result)
			if err != nil {
				return err
			}
		case "HASHES":
			result, err = ExecHash(client, &cmd, msg, result)
			if err != nil {
				return err
			}
		case "SETS":
			result, err = ExecSets(client, &cmd, msg, result)
			if err != nil {
				return err
			}
		case "SORTEDSETS":
			result, err = client.ZIncrBy(cmd.Key, 1, cmd.Value).Result()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
