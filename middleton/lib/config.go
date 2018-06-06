package lib

import "github.com/BurntSushi/toml"

type Config struct {
	Subject MetaConfig    `toml:"subject"`
	Kafka   KafkaConfig   `toml:"kafka"`
	Ledisdb LedisdbConfig `toml:"ledisdb"`
}

type MetaConfig struct {
	Limit        int    `toml:"limit"`
	Schema       string `toml:"schema"`
	Topic        string `toml:"topic"`
	Partition    int    `toml:"partition"`
	Ack          int    `toml:"ack"`
	WriteTimeout int    `toml:"write_timeout"`
}

type KafkaConfig struct {
	Brokers []Broker `toml:"broker"`
}

type Broker struct {
	Addr string `toml:"addr"`
}

type LedisdbConfig struct {
	Addr     string `toml:"addr"`
	Password string `toml:"password"`
	DB       int    `toml:"db"`
}

// DecodeConfigToml ...
func DecodeConfigToml(tomlfile string) (Config, error) {
	var config Config
	_, err := toml.DecodeFile(tomlfile, &config)
	if err != nil {
		return config, err
	}
	return config, nil
}
