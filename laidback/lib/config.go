package lib

import "github.com/BurntSushi/toml"

type Config struct {
	Main    MainConfig    `toml:"main"`
	Kafka   KafkaConfig   `toml:"kafka"`
	Ledisdb LedisdbConfig `toml:"ledisdb"`
}

type MainConfig struct {
	UpdateOffsetWait int  `toml:"update_offset_wait"`
	Debug            bool `toml:"debug"`
}

type KafkaConfig struct {
	Topics  []TopicConfig `toml:"topic"`
	Brokers []Broker      `toml:"broker"`
}

type TopicConfig struct {
	Topic      string `toml:"topic"`
	AvroSchema string `toml:"avro_schema"`
	Partitions int    `toml:"partitions"`
	Minbytes   int    `toml:"minbytes"`
	Maxbytes   int    `toml:"maxbytes"`
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
