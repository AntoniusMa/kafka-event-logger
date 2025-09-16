package producer

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
)

type mockWriter struct {
	messages []kafka.Message
	err      error
}

func (m *mockWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	m.messages = append(m.messages, msgs...)
	return m.err
}

func TestNewProducer(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := "test-topic"

	producer := NewProducer(brokers, topic)
	defer producer.Close()

	if producer == nil {
		t.Fatal("Producer should not be nil")
	}

	if producer.Topic != topic {
		t.Errorf("Expected topic %s, got %s", topic, producer.Topic)
	}
}

func TestSendMessage(t *testing.T) {
	writer := &mockWriter{}

	testKey := "test-key"
	testValue := "test-value"

	err := SendMessage(writer, testKey, testValue)

	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(writer.messages))
	}

	msg := writer.messages[0]
	if string(msg.Key) != testKey {
		t.Errorf("expected key '%s', got %s", testKey, msg.Key)
	}
	if string(msg.Value) != testValue {
		t.Errorf("expected value '%s', got %s", testValue, msg.Value)
	}
}
