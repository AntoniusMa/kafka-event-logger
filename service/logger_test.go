package service

import (
	"encoding/json"
	"kafka-logger/mocks"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestKafkaLogger(t *testing.T) {
	t.Parallel()

	t.Run("Info", func(t *testing.T) {
		mockWriter := &mocks.MockMessageWriter{}
		logger := &KafkaLogger{
			writer:  mockWriter,
			service: "test-service",
		}

		message := "This is an info message"
		fields := map[string]any{
			"user_id": 123,
			"action":  "test",
		}

		err := logger.Info(message, &fields)
		checkNoError(t, err)

		logEvent := assertLogEvent(t, mockWriter, INFO, message, "test-service")

		if logEvent.Fields["user_id"] != float64(123) {
			t.Errorf("Expected user_id 123, got %v", logEvent.Fields["user_id"])
		}

		if logEvent.Fields["action"] != "test" {
			t.Errorf("Expected action 'test', got %v", logEvent.Fields["action"])
		}
	})

	t.Run("Warn", func(t *testing.T) {
		mockWriter := &mocks.MockMessageWriter{}
		logger := &KafkaLogger{
			writer:  mockWriter,
			service: "test-service",
		}

		message := "This is a warning"
		err := logger.Warn(message, nil)
		checkNoError(t, err)

		assertLogEvent(t, mockWriter, WARN, message, "test-service")
	})

	t.Run("Error", func(t *testing.T) {
		mockWriter := &mocks.MockMessageWriter{}
		logger := &KafkaLogger{
			writer:  mockWriter,
			service: "test-service",
		}

		message := "This is an error"
		fields := map[string]interface{}{
			"error_code": 500,
			"stack":      "main.go:42",
		}

		err := logger.Error(message, &fields)
		checkNoError(t, err)

		logEvent := assertLogEvent(t, mockWriter, ERROR, message, "test-service")

		if code, ok := logEvent.Fields["error_code"].(float64); ok {
			if int(code) != 500 {
				t.Errorf("Expected error_code 500, got %v", logEvent.Fields["error_code"])
			}
		} else {
			t.Errorf("Expected error_code to be integer, but cast failed")
		}
	})

	t.Run("Debug", func(t *testing.T) {
		mockWriter := &mocks.MockMessageWriter{}
		logger := &KafkaLogger{
			writer:  mockWriter,
			service: "debug-service",
		}

		message := "Debug information"
		err := logger.Debug(message, nil)
		checkNoError(t, err)

		assertLogEvent(t, mockWriter, DEBUG, message, "debug-service")
	})

	t.Run("Without fields", func(t *testing.T) {
		mockWriter := &mocks.MockMessageWriter{}
		logger := &KafkaLogger{
			writer:  mockWriter,
			service: "test-service",
		}

		message := "Simple message"
		err := logger.Info(message, nil)
		checkNoError(t, err)

		logEvent := assertLogEvent(t, mockWriter, INFO, message, "test-service")

		if logEvent.Fields != nil {
			t.Errorf("Expected nil fields, got %v", logEvent.Fields)
		}
	})

	t.Run("Write error", func(t *testing.T) {
		expectedErr := kafka.WriteErrors{kafka.MessageTooLargeError{}}
		mockWriter := &mocks.MockMessageWriter{
			WriteErr: expectedErr,
		}
		logger := &KafkaLogger{
			writer:  mockWriter,
			service: "test-service",
		}

		err := logger.Info("test message", nil)

		if err == nil {
			t.Error("Expected an error, got nil")
		}

		if err.Error() != expectedErr.Error() {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("LogEvent timestamp", func(t *testing.T) {
		mockWriter := &mocks.MockMessageWriter{}
		logger := &KafkaLogger{
			writer:  mockWriter,
			service: "test-service",
		}

		before := time.Now().UTC()
		err := logger.Info("timestamp test", nil)
		after := time.Now().UTC()

		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		var logEvent LogEvent
		err = json.Unmarshal(mockWriter.Messages[0].Value, &logEvent)
		if err != nil {
			t.Errorf("Failed to unmarshal log event: %v", err)
		}

		if logEvent.Timestamp.Before(before) || logEvent.Timestamp.After(after) {
			t.Errorf("Timestamp %v should be between %v and %v", logEvent.Timestamp, before, after)
		}

		if logEvent.Timestamp.Location() != time.UTC {
			t.Error("Timestamp should be in UTC")
		}
	})
}

func TestNewKafkaLogger(t *testing.T) {
	brokers := []string{"localhost:9092", "localhost:9093"}
	topic := "test-topic"
	serviceName := "test-service"

	logger := NewKafkaLogger(brokers, topic, serviceName)

	if logger == nil {
		t.Error("Expected logger to be created, got nil")
	}

	if logger.service != serviceName {
		t.Errorf("Expected service name '%s', got '%s'", serviceName, logger.service)
	}

	if logger.writer == nil {
		t.Error("Expected writer to be initialized, got nil")
	}
}

func TestClose(t *testing.T) {
	t.Run("With closer function", func(t *testing.T) {
		var closeCalledFromCloseFunc = false
		mockWriter := &mocks.MockClosableWriter{
			MockMessageWriter: &mocks.MockMessageWriter{},
			CloseFunc: func() error {
				closeCalledFromCloseFunc = true
				return nil
			},
		}

		logger := &KafkaLogger{
			writer:  mockWriter,
			service: "test-service",
		}

		err := logger.Close()

		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if !mockWriter.CloseCalled {
			t.Error("Expected Close to be called on writer")
		}

		if !closeCalledFromCloseFunc {
			t.Error("Expected Close to be called with CloseFunc")
		}
	})

	t.Run("Without closer function", func(t *testing.T) {
		mockWriter := &mocks.MockMessageWriter{}
		logger := &KafkaLogger{
			writer:  mockWriter,
			service: "test-service",
		}

		err := logger.Close()

		if err != nil {
			t.Errorf("Expected no error when writer doesn't implement Close, got: %v", err)
		}
	})
}

func checkNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
}

func assertLogEvent(t *testing.T, mockWriter *mocks.MockMessageWriter, expectedLevel LogLevel, expectedMessage, expectedService string) LogEvent {
	t.Helper()

	if len(mockWriter.Messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(mockWriter.Messages))
	}

	msg := mockWriter.Messages[0]
	if string(msg.Key) != expectedService {
		t.Errorf("Expected key '%s', got '%s'", expectedService, string(msg.Key))
	}

	var logEvent LogEvent
	if err := json.Unmarshal(msg.Value, &logEvent); err != nil {
		t.Fatalf("Failed to unmarshal log event: %v", err)
	}

	if logEvent.Level != expectedLevel {
		t.Errorf("Expected level %s, got %s", expectedLevel, logEvent.Level)
	}

	if logEvent.Message != expectedMessage {
		t.Errorf("Expected message '%s', got '%s'", expectedMessage, logEvent.Message)
	}

	if logEvent.Service != expectedService {
		t.Errorf("Expected service '%s', got '%s'", expectedService, logEvent.Service)
	}

	if time.Since(logEvent.Timestamp) > time.Millisecond {
		t.Error("Timestamp should be recent")
	}

	return logEvent
}
