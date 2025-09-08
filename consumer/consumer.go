package consumer

import (
	"context"
	"fmt"
	"io"

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

func ConsumeMessages(ctx context.Context, reader MessageReader, writer io.Writer) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				return err
			}
			fmt.Fprintf(writer, "Key: %s, Value: %s\n", string(message.Key), string(message.Value))
		}
	}
}