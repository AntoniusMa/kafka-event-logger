package consumer

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type MessageReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

func NewConsumer(brokers []string, topic, groupID string) MessageReader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	return reader
}

func ConsumeMessages(reader MessageReader) error {
	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			return err
		}
		fmt.Printf("Key: %s, Value: %s\n", string(message.Key), string(message.Value))
	}
}