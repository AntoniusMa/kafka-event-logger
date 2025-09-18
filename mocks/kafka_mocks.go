package mocks

import (
	"context"
	"errors"
	"io"

	"github.com/segmentio/kafka-go"
)

// MockMessageWriter extends MockMessageWriter to implement Close for testing Close functionality
type MockMessageWriter struct {
	Messages    []kafka.Message
	WriteErr    error
	CloseFunc   func() error
	CloseCalled bool
}

func (m *MockMessageWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.WriteErr != nil {
		return m.WriteErr
	}
	m.Messages = append(m.Messages, msgs...)
	return nil
}

func (m *MockMessageWriter) Close() error {
	m.CloseCalled = true
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

// MockMessageReader implements the consumer.MessageReader interface for testing
type MockMessageReader struct {
	Messages    []kafka.Message
	Index       int
	ShouldError bool
	ErrorMsg    string
	CloseCalled bool
}

func (m *MockMessageReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if m.ShouldError {
		if m.ErrorMsg != "" {
			return kafka.Message{}, errors.New(m.ErrorMsg)
		}
		return kafka.Message{}, errors.New("mock error")
	}
	if m.Index >= len(m.Messages) {
		return kafka.Message{}, io.EOF
	}
	msg := m.Messages[m.Index]
	m.Index++
	return msg, nil
}

func (m *MockMessageReader) Close() error {
	m.CloseCalled = true
	return nil
}
