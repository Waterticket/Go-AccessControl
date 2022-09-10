package main

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// DatabaseConfig Type
type RedisConfig struct {
	Addr     string `yaml:"Addr"`
	Password string `yaml:"Password"`
	DB       int    `yaml:"DB"`
}

type ConnectionConfig struct {
	AccessSize int64 `yaml:"AccessSize"`
	TTL        int64 `yaml:"TTL"`
}

type ServerConfig struct {
	Number          uint16 `yaml:"Number"`
	Master          bool   `yaml:"Master"`
	WaitingHTMLFile string `yaml:"WaitingHTMLFile"`
}

type Config struct {
	Redis      RedisConfig      `yaml:"Redis"`
	Connection ConnectionConfig `yaml:"Connection"`
	Server     ServerConfig     `yaml:"Server"`
}

var config Config

func init() {
	filename, _ := filepath.Abs("config.yaml")
	yamlFile, err := os.ReadFile(filename)

	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(yamlFile, &config)

	if err != nil {
		panic(err)
	}
}
