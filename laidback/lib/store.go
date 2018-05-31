package lib

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/go-redis/redis"
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

// storeMessage ...
func storeMessage(client *redis.Client, cmd, key, body string) error {
	switch cmd {
	case "LIST":
		if err := client.RPush(key, body).Err(); err != nil {
			return err
		}
	case "SET":
		if err := client.Set(key, body, 0).Err(); err != nil {
			return err
		}
	case "ZADD":
		if err := client.ZIncrBy(key, 1, body).Err(); err != nil {
			return err
		}
	default:
		return errors.New(fmt.Sprintf("command no match error. [%s]", cmd))
	}
	return nil
}
