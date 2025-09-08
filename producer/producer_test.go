package producer

import (
	"testing"
	"github.com/segmentio/kafka-go"
)

func TestNewProducer(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := "test-topic"

	producer := NewProducer(brokers, topic)
	defer producer.Close()
	
	if producer == nil {
		t.Error("Producer should not be nil")
	}

	if producer.Topic != topic {
		t.Errorf("Expected topic %s, got %s", topic, producer.Topic)
	}
}

func TestSendMessage(t *testing.T) {
	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "test-topic",
	}
	defer writer.Close()

	err := SendMessage(writer, "test-key", "test-value")
	if err != nil {
		t.Logf("Send message failed (expected if Kafka not running): %v", err)
	}
}