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
	AccessSize            int64 `yaml:"AccessSize"`
	BucketTTLMilliseconds int64 `yaml:"BucketTTLMilliseconds"`
}

type ServerConfig struct {
	Sequence        uint16 `yaml:"Sequence"`
	Master          bool   `yaml:"Master"`
	Addr            string `yaml:"Addr"`
	WaitingHTMLFile string `yaml:"WaitingHTMLFile"`
}

type ProxyConfig struct {
	Addr                      string `yaml:"Addr"`
	TimeoutMilliseconds       int64  `yaml:"TimeoutMilliseconds"`
	PoolConnectionCapacityMin int    `yaml:"PoolConnectionCapacityMin"`
	PoolConnectionCapacityMax int    `yaml:"PoolConnectionCapacityMax"`
}

type ClientConfig struct {
	HeartbeatRedisTTLMilliseconds int64 `yaml:"HeartbeatRedisTTLMilliseconds"`
}

type Config struct {
	Redis      RedisConfig      `yaml:"Redis"`
	Connection ConnectionConfig `yaml:"Connection"`
	Server     ServerConfig     `yaml:"Server"`
	Client     ClientConfig     `yaml:"Client"`
	Proxy      ProxyConfig      `yaml:"Proxy"`
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
