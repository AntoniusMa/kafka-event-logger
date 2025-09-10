package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"kafka-logger/service"
	"time"

	"github.com/segmentio/kafka-go"
)

type MessageReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

func NewConsumer(brokers []string, topic, groupID string) MessageReader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	return reader
}

func ConsumeRawMessages(ctx context.Context, reader MessageReader, writer io.Writer) error {
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

func ConsumeLogEvents(ctx context.Context, reader MessageReader, writer io.Writer) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				return err
			}

			var logEvent service.LogEvent
			if err := json.Unmarshal(message.Value, &logEvent); err != nil {
				fmt.Fprintf(writer, "Error parsing log event: %v, Raw message: %s\n", err, string(message.Value))
				continue
			}

			timestamp := logEvent.Timestamp.Format(time.RFC3339)

			if len(logEvent.Fields) > 0 {
				fieldsStr := ""
				for key, value := range logEvent.Fields {
					fieldsStr += fmt.Sprintf(" %s=%v", key, value)
				}
				fmt.Fprintf(writer, "%s [%s] %s: %s%s\n", timestamp, logEvent.Level, logEvent.Service, logEvent.Message, fieldsStr)
			} else {
				fmt.Fprintf(writer, "%s [%s] %s: %s\n", timestamp, logEvent.Level, logEvent.Service, logEvent.Message)
			}
		}
	}
}
