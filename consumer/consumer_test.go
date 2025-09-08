package consumer

import (
	"context"
	"errors"
	"io"
	"testing"
	"github.com/segmentio/kafka-go"
)

// MockReader implements the MessageReader interface for testing
type MockReader struct {
	messages    []kafka.Message
	index       int
	shouldError bool
	errorMsg    string
}

func (m *MockReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if m.shouldError {
		if m.errorMsg != "" {
			return kafka.Message{}, errors.New(m.errorMsg)
		}
		return kafka.Message{}, errors.New("mock error")
	}
	if m.index >= len(m.messages) {
		return kafka.Message{}, io.EOF
	}
	msg := m.messages[m.index]
	m.index++
	return msg, nil
}

func (m *MockReader) Close() error {
	return nil
}

func TestNewConsumer(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := "test-topic"
	groupID := "test-group"

	consumer := NewConsumer(brokers, topic, groupID)
	if consumer == nil {
		t.Error("Consumer should not be nil")
	}
}

func TestConsumeMessages(t *testing.T) {
	t.Parallel()
	
	t.Run("success", func(t *testing.T) {
		t.Parallel()
		mockMessages := []kafka.Message{
			{Key: []byte("key1"), Value: []byte("value1")},
		}
		
		mockReader := &MockReader{
			messages: mockMessages,
			index:    0,
		}
		
		err := ConsumeMessages(mockReader)
		if err != io.EOF {
			t.Errorf("Expected EOF error, got: %v", err)
		}
	})
	
	t.Run("error", func(t *testing.T) {
		t.Parallel()
		mockReader := &MockReader{
			shouldError: true,
			errorMsg:    "test error",
		}
		
		err := ConsumeMessages(mockReader)
		if err == nil {
			t.Error("Expected error but got nil")
		}
		if err.Error() != "test error" {
			t.Errorf("Expected 'test error', got: %v", err)
		}
	})
}