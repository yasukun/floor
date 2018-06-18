package lib

import "github.com/BurntSushi/toml"

type Config struct {
	Subject  MetaConfig    `toml:"subject"`
	Activity MetaConfig    `toml:"activity"`
	Comment  MetaConfig    `toml:"comment"`
	Metainfo MetaConfig    `toml:"metainfo"`
	Kafka    KafkaConfig   `toml:"kafka"`
	Ledisdb  LedisdbConfig `toml:"ledisdb"`
}

type MetaConfig struct {
	Limit     int    `toml:"limit"`
	Schema    string `toml:"schema"`
	Topic     string `toml:"topic"`
	Partition int    `toml:"partition"`
	Ack       int    `toml:"ack"`
}

type KafkaConfig struct {
	Minbytes     int      `toml:"minbytes"`
	Maxbytes     int      `toml:"maxbytes"`
	Cancel       int      `toml:"cancel"`
	MaxWait      int      `toml:"maxwait"`
	WriteTimeout int      `toml:"write_timeout"`
	Brokers      []Broker `toml:"broker"`
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
