package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka    KafkaConfig `yaml:"kafka"`
	Logging  LogConfig   `yaml:"logging"`
	Consumer ConsumerConfig `yaml:"consumer"`
}

type KafkaConfig struct {
	Brokers      []string `yaml:"brokers"`
	Topic        string   `yaml:"topic"`
	Partitions   int      `yaml:"partitions"`
}

type LogConfig struct {
	ServiceName string `yaml:"service_name"`
	FilePath    string `yaml:"file_path"`
}

type ConsumerConfig struct {
	GroupName    string `yaml:"group_name"`
	NumConsumers int    `yaml:"num_consumers"`
}

func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

func DefaultConfig() *Config {
	return &Config{
		Kafka: KafkaConfig{
			Brokers:    []string{"localhost:9092"},
			Topic:      "logs-topic",
			Partitions: 3,
		},
		Logging: LogConfig{
			ServiceName: "demo-service",
			FilePath:    "./logs",
		},
		Consumer: ConsumerConfig{
			GroupName:    "logger-group",
			NumConsumers: 3,
		},
	}
}