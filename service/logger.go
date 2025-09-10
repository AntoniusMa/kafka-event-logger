package service

import (
	"context"
	"encoding/json"
	"kafka-logger/producer"
	"time"

	"github.com/segmentio/kafka-go"
)

type LogLevel string

const (
	INFO  LogLevel = "INFO"
	WARN  LogLevel = "WARN"
	ERROR LogLevel = "ERROR"
	DEBUG LogLevel = "DEBUG"
)

type LogEvent struct {
	Timestamp time.Time      `json:"timestamp"`
	Level     LogLevel       `json:"level"`
	Message   string         `json:"message"`
	Service   string         `json:"service"`
	Fields    map[string]any `json:"fields,omitempty"`
}

type KafkaLogger struct {
	writer  producer.MessageWriter
	service string
}

func NewKafkaLogger(brokers []string, topic, serviceName string) *KafkaLogger {
	return &KafkaLogger{
		writer:  producer.NewProducer(brokers, topic),
		service: serviceName,
	}
}

func (kl *KafkaLogger) log(level LogLevel, message string, fields *map[string]any) error {
	var f map[string]any
	if fields != nil {
		f = *fields
	}

	event := LogEvent{
		Timestamp: time.Now().UTC(),
		Level:     level,
		Message:   message,
		Service:   kl.service,
		Fields:    f,
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return kl.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(kl.service),
			Value: data,
		},
	)
}

func (kl *KafkaLogger) Info(message string, fields *map[string]any) error {
	return kl.log(INFO, message, fields)
}

func (kl *KafkaLogger) Warn(message string, fields *map[string]any) error {
	return kl.log(WARN, message, fields)
}

func (kl *KafkaLogger) Error(message string, fields *map[string]any) error {
	return kl.log(ERROR, message, fields)
}

func (kl *KafkaLogger) Debug(message string, fields *map[string]any) error {
	return kl.log(DEBUG, message, fields)
}

func (kl *KafkaLogger) Close() error {
	if closer, ok := kl.writer.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
