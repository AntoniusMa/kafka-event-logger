package consumer

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func NewConsumer(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
}

func ConsumeMessages(reader *kafka.Reader) error {
	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			return err
		}
		fmt.Printf("Key: %s, Value: %s\n", string(message.Key), string(message.Value))
	}
}