package producer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

func NewProducer(brokers []string, topic string) *kafka.Writer {

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	return writer
}

func SendMessage(writer MessageWriter, key, value string) error {
	return writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: []byte(value),
		},
	)
}
