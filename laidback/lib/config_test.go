package lib

import (
	"log"
	"testing"
)

// TestDecodeConfigToml ...
func TestDecodeConfigToml(t *testing.T) {
	cfg, err := DecodeConfigToml("../laidback.toml")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(cfg)
}
