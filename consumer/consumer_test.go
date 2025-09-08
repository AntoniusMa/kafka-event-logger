package consumer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

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
			{Key: []byte("key2"), Value: []byte("value2")},
		}
		
		mockReader := &MockReader{
			messages: mockMessages,
			index:    0,
		}
		
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		
		var buf bytes.Buffer
		err := ConsumeMessages(ctx, mockReader, &buf)
		if err != io.EOF && err != context.DeadlineExceeded {
			t.Errorf("Expected EOF or context timeout, got: %v", err)
		}
		
		expectedOutput := "Key: key1, Value: value1\nKey: key2, Value: value2\n"
		if buf.String() != expectedOutput {
			t.Errorf("Expected output '%s', got: '%s'", expectedOutput, buf.String())
		}
	})
	
	t.Run("error", func(t *testing.T) {
		t.Parallel()
		mockReader := &MockReader{
			shouldError: true,
			errorMsg:    "test error",
		}
		
		ctx := context.Background()
		var buf bytes.Buffer
		err := ConsumeMessages(ctx, mockReader, &buf)
		if err == nil {
			t.Error("Expected error but got nil")
		}
		if err.Error() != "test error" {
			t.Errorf("Expected 'test error', got: %v", err)
		}
	})
	
	t.Run("context_cancellation", func(t *testing.T) {
		t.Parallel()
		mockMessages := []kafka.Message{
			{Key: []byte("key1"), Value: []byte("value1")},
		}
		
		mockReader := &MockReader{
			messages: mockMessages,
			index:    0,
		}
		
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		var buf bytes.Buffer
		err := ConsumeMessages(ctx, mockReader, &buf)
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	})
}