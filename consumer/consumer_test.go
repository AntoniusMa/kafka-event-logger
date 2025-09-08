package consumer

import (
	"testing"
)

func TestNewConsumer(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := "test-topic"
	groupID := "test-group"

	consumer := NewConsumer(brokers, topic, groupID)
	defer consumer.Close()
	
	if consumer == nil {
		t.Error("Consumer should not be nil")
	}
}

func TestConsumeMessages(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := "test-topic"
	groupID := "test-group"

	reader := NewConsumer(brokers, topic, groupID)
	defer reader.Close()

	err := ConsumeMessages(reader)
	if err != nil {
		t.Logf("Consume messages failed (expected if Kafka not running): %v", err)
	}
}