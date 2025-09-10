package main

import (
	"context"
	"kafka-logger/consumer"
	"kafka-logger/service"
	"log"
	"os"
	"time"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "logs-topic"

	initKafkaTopic(brokers, topic)

	logger := service.NewKafkaLogger(brokers, topic, "demo-service")
	defer logger.Close()

	logger.Info("Application started", nil)

	warnFields := map[string]any{
		"user_id": 12345,
		"action":  "login_attempt",
	}
	logger.Warn("This is a warning message", &warnFields)

	errorFields := map[string]any{
		"error":       "connection timeout",
		"database":    "postgres",
		"retry_count": 3,
	}
	logger.Error("Database connection failed", &errorFields)

	debugFields := map[string]any{
		"request_id": "req-123",
		"endpoint":   "/api/users",
	}
	logger.Debug("Processing user request", &debugFields)

	time.Sleep(time.Second)

	c := consumer.NewConsumer(brokers, topic, "logger-group")
	defer c.Close()

	ctx := context.Background()
	if err := consumer.ConsumeLogEvents(ctx, c, os.Stdout); err != nil {
		log.Fatal(err)
	}
}
