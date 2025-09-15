package main

import (
	"context"
	"kafka-logger/consumer"
	"kafka-logger/filewriter"
	"kafka-logger/service"
	"log"
	"sync"
	"time"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "logs-topic"

	initKafkaTopic(brokers, topic, 3)

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

	logWriter := filewriter.NewLogFileWriter("./logs")
	defer logWriter.Close()

	ctx := context.Background()
	numConsumers := 3
	var wg sync.WaitGroup

	for i := range numConsumers {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			c := consumer.NewConsumer(brokers, topic, "logger-group")
			defer c.Close()

			log.Printf("Starting consumer %d", consumerID)
			if err := consumer.ConsumeLogEventsToFiles(ctx, c, logWriter); err != nil {
				log.Printf("Consumer %d error: %v", consumerID, err)
			}
		}(i)
	}

	wg.Wait()
}
