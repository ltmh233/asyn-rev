package config

import (
	"context"
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	DB       int    `yaml:"db"`
	Password string `yaml:"password"`
}

type ServerConfig struct {
	Host string `yaml:"host"`
	Port string `yaml:"port"`
}

type Config struct {
	Redis  RedisConfig  `yaml:"redis"`
	Server ServerConfig `yaml:"server"`
}

func LoadConfig(ctx context.Context) (Config, error) {
	config := Config{}
	data, err := os.ReadFile("./config.yaml")

	if err != nil {
		return config, fmt.Errorf("error occur while config loadconfig os.ReadFile: %w", err)
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return config, fmt.Errorf("error occur while config loadconfig yaml.Unmarshal: %w", err)
	}
	return config, err
}
