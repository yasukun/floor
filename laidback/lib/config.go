package lib

import "github.com/BurntSushi/toml"

type Config struct {
	Main    MainConfig    `toml:"main"`
	Kafka   KafkaConfig   `toml:"kafka"`
	Ledisdb LedisdbConfig `toml:"ledisdb"`
	Avro    AvroConfig    `toml:"avro"`
}

type MainConfig struct {
	UpdateOffsetWait int  `toml:"update_offset_wait"`
	Debug            bool `toml:"debug"`
}

type KafkaConfig struct {
	Topic      string   `toml:"topic"`
	Schema     string   `toml:"schema"`
	Partitions int      `toml:"partitions"`
	Minbytes   int      `toml:"minbytes"`
	Maxbytes   int      `toml:"maxbytes"`
	Brokers    []Broker `toml:"broker"`
}

type Broker struct {
	Addr string `toml:"addr"`
}

type LedisdbConfig struct {
	Addr     string `toml:"addr"`
	Password string `toml:"password"`
	DB       int    `toml:"db"`
}

type AvroConfig struct {
	Schema string `toml:"schema"`
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
