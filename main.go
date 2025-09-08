package main

import (
	"context"
	"log"
	"os"
	"kafka-logger/consumer"
	"kafka-logger/producer"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "logs-topic"

	p := producer.NewProducer(brokers, topic)
	defer p.Close()

	if err := producer.SendMessage(p, "log-key", "Hello Kafka Logger!"); err != nil {
		log.Fatal(err)
	}

	c := consumer.NewConsumer(brokers, topic, "logger-group")
	defer c.Close()

	ctx := context.Background()
	if err := consumer.ConsumeMessages(ctx, c, os.Stdout); err != nil {
		log.Fatal(err)
	}
}