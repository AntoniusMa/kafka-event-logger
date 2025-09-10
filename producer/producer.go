package producer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

func NewProducer(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func SendMessage(writer MessageWriter, key, value string) error {
	return writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: []byte(value),
		},
	)
}
