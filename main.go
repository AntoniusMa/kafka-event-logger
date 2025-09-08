package main

import (
	"log"
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

	if err := consumer.ConsumeMessages(c); err != nil {
		log.Fatal(err)
	}
}