package main

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

type MockConn struct {
	createTopicsFunc func(topics ...kafka.TopicConfig) error
	closeFunc        func() error
}

func (m *MockConn) CreateTopics(topics ...kafka.TopicConfig) error {
	if m.createTopicsFunc != nil {
		return m.createTopicsFunc(topics...)
	}
	return nil
}

func (m *MockConn) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

type MockDialer struct {
	dialFunc func(ctx context.Context, network, address string) (KafkaConn, error)
}

func (m *MockDialer) DialContext(ctx context.Context, network, address string) (KafkaConn, error) {
	if m.dialFunc != nil {
		return m.dialFunc(ctx, network, address)
	}
	return &MockConn{}, nil
}

func TestCreateTopic(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		var createdTopic kafka.TopicConfig
		mockConn := &MockConn{
			createTopicsFunc: func(topics ...kafka.TopicConfig) error {
				if len(topics) > 0 {
					createdTopic = topics[0]
				}
				return nil
			},
		}

		mockDialer := &MockDialer{
			dialFunc: func(ctx context.Context, network, address string) (KafkaConn, error) {
				return mockConn, nil
			},
		}

		brokers := []string{"localhost:9092"}
		topic := "test-topic"

		err := createTopicWithDialer(mockDialer, brokers, topic)

		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if createdTopic.Topic != topic {
			t.Errorf("Expected topic '%s', got '%s'", topic, createdTopic.Topic)
		}

		if createdTopic.NumPartitions != 1 {
			t.Errorf("Expected 1 partition, got %d", createdTopic.NumPartitions)
		}

		if createdTopic.ReplicationFactor != 1 {
			t.Errorf("Expected replication factor 1, got %d", createdTopic.ReplicationFactor)
		}
	})

	t.Run("DialError", func(t *testing.T) {
		expectedErr := errors.New("dial failed")
		mockDialer := &MockDialer{
			dialFunc: func(ctx context.Context, network, address string) (KafkaConn, error) {
				return nil, expectedErr
			},
		}

		brokers := []string{"localhost:9092"}
		topic := "test-topic"

		err := createTopicWithDialer(mockDialer, brokers, topic)

		if err != expectedErr {
			t.Errorf("Expected error '%v', got '%v'", expectedErr, err)
		}
	})

	t.Run("CreateTopicsError", func(t *testing.T) {
		expectedErr := errors.New("create topics failed")
		mockConn := &MockConn{
			createTopicsFunc: func(topics ...kafka.TopicConfig) error {
				return expectedErr
			},
		}

		mockDialer := &MockDialer{
			dialFunc: func(ctx context.Context, network, address string) (KafkaConn, error) {
				return mockConn, nil
			},
		}

		brokers := []string{"localhost:9092"}
		topic := "test-topic"

		err := createTopicWithDialer(mockDialer, brokers, topic)

		if err != expectedErr {
			t.Errorf("Expected error '%v', got '%v'", expectedErr, err)
		}
	})

	t.Run("Connection closed", func(t *testing.T) {
		closeCalled := false
		mockConn := &MockConn{
			closeFunc: func() error {
				closeCalled = true
				return nil
			},
		}

		mockDialer := &MockDialer{
			dialFunc: func(ctx context.Context, network, address string) (KafkaConn, error) {
				return mockConn, nil
			},
		}

		brokers := []string{"localhost:9092"}
		topic := "test-topic"

		_ = createTopicWithDialer(mockDialer, brokers, topic)

		if !closeCalled {
			t.Error("Expected connection to be closed")
		}
	})
}
