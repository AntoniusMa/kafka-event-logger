package main

import (
	"context"
	"kafka-logger/consumer"
	"kafka-logger/filewriter"
	"kafka-logger/service"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
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

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

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
				if err != context.Canceled {
					log.Printf("Consumer %d error: %v", consumerID, err)
				} else {
					log.Printf("Consumer %d shutdown gracefully", consumerID)
				}
			}
		}(i)
	}

	wg.Wait()
	log.Println("All consumers stopped, application shutdown complete")
}
