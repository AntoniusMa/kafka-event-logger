package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type KafkaConn interface {
	CreateTopics(topics ...kafka.TopicConfig) error
	Close() error
}

type ConnDialer interface {
	DialContext(ctx context.Context, network, address string) (KafkaConn, error)
}

type DefaultDialer struct{}

func (d DefaultDialer) DialContext(ctx context.Context, network, address string) (KafkaConn, error) {
	return kafka.DialContext(ctx, network, address)
}

func createTopicWithDialer(dialer ConnDialer, brokers []string, topic string) error {
	conn, err := dialer.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = conn.CreateTopics(topicConfig)
	if err != nil {
		return err
	}

	log.Printf("Topic '%s' created successfully", topic)
	return nil
}

func createTopic(brokers []string, topic string) error {
	return createTopicWithDialer(DefaultDialer{}, brokers, topic)
}

func initKafkaTopic(brokers []string, topic string) {
	if err := createTopic(brokers, topic); err != nil {
		log.Printf("Warning: Failed to create topic '%s': %v", topic, err)
	}
}
