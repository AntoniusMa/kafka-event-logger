package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"kafka-logger/mocks"
	"kafka-logger/service"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestNewConsumer(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := "test-topic"
	groupID := "test-group"

	consumer := NewConsumer(brokers, topic, groupID)
	if consumer == nil {
		t.Error("Consumer should not be nil")
	}
}

func TestConsumePlainMessages(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		testKey1 := "key1"
		testValue1 := "value1"
		testKey2 := "key2"
		testValue2 := "value2"

		mockMessages := []kafka.Message{
			{Key: []byte(testKey1), Value: []byte(testValue1)},
			{Key: []byte(testKey2), Value: []byte(testValue2)},
		}

		mockReader := &mocks.MockMessageReader{
			Messages: mockMessages,
			Index:    0,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		var buf bytes.Buffer
		err := ConsumeRawMessages(ctx, mockReader, &buf)
		if err != io.EOF && err != context.DeadlineExceeded {
			t.Errorf("Expected EOF or context timeout, got: %v", err)
		}

		expectedOutput := fmt.Sprintf("Key: %s, Value: %s\nKey: %s, Value: %s\n", testKey1, testValue1, testKey2, testValue2)
		if buf.String() != expectedOutput {
			t.Errorf("Expected output '%s', got: '%s'", expectedOutput, buf.String())
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()
		expectedErrorMessage := "test error"

		mockReader := &mocks.MockMessageReader{
			ShouldError: true,
			ErrorMsg:    expectedErrorMessage,
		}

		ctx := context.Background()
		var buf bytes.Buffer
		err := ConsumeRawMessages(ctx, mockReader, &buf)
		if err == nil {
			t.Error("Expected error but got nil")
		}
		if err.Error() != expectedErrorMessage {
			t.Errorf("Expected '%s', got: %v", expectedErrorMessage, err)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		t.Parallel()
		mockMessages := []kafka.Message{
			{Key: []byte("key1"), Value: []byte("value1")},
		}

		mockReader := &mocks.MockMessageReader{
			Messages: mockMessages,
			Index:    0,
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		var buf bytes.Buffer
		err := ConsumeRawMessages(ctx, mockReader, &buf)
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	})
}

func TestConsumeLogEvents(t *testing.T) {
	t.Parallel()

	t.Run("Success with fields", func(t *testing.T) {
		t.Parallel()
		mockTimestamp := time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)
		testLogLevel := service.INFO
		testMessage := "Test message"
		testServiceKey := "test-service"
		testUserID := 123
		testAction := "login"

		logEvent := service.LogEvent{
			Timestamp: mockTimestamp,
			Level:     testLogLevel,
			Message:   testMessage,
			Service:   testServiceKey,
			Fields: map[string]any{
				"user_id": testUserID,
				"action":  testAction,
			},
		}

		jsonData, err := json.Marshal(logEvent)
		if err != nil {
			t.Fatalf("Failed to marshal log event: %v", err)
		}

		mockMessages := []kafka.Message{
			{Key: []byte(testServiceKey), Value: jsonData},
		}

		mockReader := &mocks.MockMessageReader{
			Messages: mockMessages,
			Index:    0,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		var buf bytes.Buffer
		err = ConsumeLogEvents(ctx, mockReader, &buf)
		if err != io.EOF && err != context.DeadlineExceeded {
			t.Errorf("Expected EOF or context timeout, got: %v", err)
		}

		output := buf.String()

		assertBasicLogOutput(t, output, mockTimestamp, testLogLevel, testServiceKey, testMessage)

		expectedUserIDStr := fmt.Sprintf("user_id=%d", testUserID)
		expectedActionStr := fmt.Sprintf("action=%s", testAction)

		if !strings.Contains(output, expectedUserIDStr) {
			t.Errorf("Expected user_id field %s in output, got: %s", expectedUserIDStr, output)
		}
		if !strings.Contains(output, expectedActionStr) {
			t.Errorf("Expected action field %s in output, got: %s", expectedActionStr, output)
		}
	})

	t.Run("Success without fields", func(t *testing.T) {
		t.Parallel()
		mockTimestamp := time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)
		testLogLevel := service.WARN
		testMessage := "Warning message"
		testServiceKey := "warning-service"

		logEvent := service.LogEvent{
			Timestamp: mockTimestamp,
			Level:     testLogLevel,
			Message:   testMessage,
			Service:   testServiceKey,
			Fields:    nil,
		}

		jsonData, err := json.Marshal(logEvent)
		if err != nil {
			t.Fatalf("Failed to marshal log event: %v", err)
		}

		mockMessages := []kafka.Message{
			{Key: []byte(testServiceKey), Value: jsonData},
		}

		mockReader := &mocks.MockMessageReader{
			Messages: mockMessages,
			Index:    0,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		var buf bytes.Buffer
		err = ConsumeLogEvents(ctx, mockReader, &buf)
		if err != io.EOF && err != context.DeadlineExceeded {
			t.Errorf("Expected EOF or context timeout, got: %v", err)
		}

		output := buf.String()
		expectedTimestampStr := mockTimestamp.Format(time.RFC3339)
		expectedOutput := fmt.Sprintf("%s [%s] %s: %s\n", expectedTimestampStr, testLogLevel, testServiceKey, testMessage)

		if strings.TrimSpace(output) != strings.TrimSpace(expectedOutput) {
			t.Errorf("Expected output '%s', got '%s'", expectedOutput, output)
		}
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		t.Parallel()
		testServiceKey := "test-service"
		invalidJSON := "invalid json"

		mockMessages := []kafka.Message{
			{Key: []byte(testServiceKey), Value: []byte(invalidJSON)},
		}

		mockReader := &mocks.MockMessageReader{
			Messages: mockMessages,
			Index:    0,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		var buf bytes.Buffer
		err := ConsumeLogEvents(ctx, mockReader, &buf)
		if err != io.EOF && err != context.DeadlineExceeded {
			t.Errorf("Expected EOF or context timeout, got: %v", err)
		}

		output := buf.String()
		expectedErrorPrefix := "Error parsing log event:"

		if !strings.Contains(output, expectedErrorPrefix) {
			t.Errorf("Expected error message %s in output, got: %s", expectedErrorPrefix, output)
		}
		if !strings.Contains(output, invalidJSON) {
			t.Errorf("Expected raw message %s in output, got: %s", invalidJSON, output)
		}
	})

	t.Run("Multiple log levels", func(t *testing.T) {
		t.Parallel()
		testServiceKey := "test-service"
		infoTimestamp := time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)
		errorTimestamp := time.Date(2024, 1, 15, 10, 30, 46, 0, time.UTC)
		infoLevel := service.INFO
		errorLevel := service.ERROR
		infoMessage := "Info message"
		errorMessage := "Error message"
		errorCode := 500

		events := []service.LogEvent{
			{
				Timestamp: infoTimestamp,
				Level:     infoLevel,
				Message:   infoMessage,
				Service:   testServiceKey,
			},
			{
				Timestamp: errorTimestamp,
				Level:     errorLevel,
				Message:   errorMessage,
				Service:   testServiceKey,
				Fields: map[string]any{
					"error_code": errorCode,
				},
			},
		}

		var mockMessages []kafka.Message
		for _, event := range events {
			jsonData, err := json.Marshal(event)
			if err != nil {
				t.Fatalf("Failed to marshal log event: %v", err)
			}
			mockMessages = append(mockMessages, kafka.Message{
				Key:   []byte(testServiceKey),
				Value: jsonData,
			})
		}

		mockReader := &mocks.MockMessageReader{
			Messages: mockMessages,
			Index:    0,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		var buf bytes.Buffer
		err := ConsumeLogEvents(ctx, mockReader, &buf)
		if err != io.EOF && err != context.DeadlineExceeded {
			t.Errorf("Expected EOF or context timeout, got: %v", err)
		}

		output := buf.String()

		// Check the complete expected output
		infoTimestampStr := infoTimestamp.Format(time.RFC3339)
		errorTimestampStr := errorTimestamp.Format(time.RFC3339)
		expectedErrorCodeStr := fmt.Sprintf("error_code=%d", errorCode)
		
		expectedCompleteOutput := fmt.Sprintf("%s [%s] %s: %s\n%s [%s] %s: %s %s\n", 
			infoTimestampStr, infoLevel, testServiceKey, infoMessage,
			errorTimestampStr, errorLevel, testServiceKey, errorMessage, expectedErrorCodeStr)

		if output != expectedCompleteOutput {
			t.Errorf("Expected complete output:\n'%s'\nGot:\n'%s'", expectedCompleteOutput, output)
		}
	})

	t.Run("Read error", func(t *testing.T) {
		t.Parallel()
		expectedErrorMessage := "read error"

		mockReader := &mocks.MockMessageReader{
			ShouldError: true,
			ErrorMsg:    expectedErrorMessage,
		}

		ctx := context.Background()
		var buf bytes.Buffer
		err := ConsumeLogEvents(ctx, mockReader, &buf)

		if err == nil {
			t.Error("Expected error but got nil")
		}
		if err.Error() != expectedErrorMessage {
			t.Errorf("Expected '%s', got: %v", expectedErrorMessage, err)
		}
	})

	t.Run("Context cancellation", func(t *testing.T) {
		t.Parallel()
		testLogLevel := service.DEBUG
		testMessage := "Debug message"
		testServiceKey := "debug-service"

		logEvent := service.LogEvent{
			Timestamp: time.Now().UTC(),
			Level:     testLogLevel,
			Message:   testMessage,
			Service:   testServiceKey,
		}

		jsonData, err := json.Marshal(logEvent)
		if err != nil {
			t.Fatalf("Failed to marshal log event: %v", err)
		}

		mockMessages := []kafka.Message{
			{Key: []byte(testServiceKey), Value: jsonData},
		}

		mockReader := &mocks.MockMessageReader{
			Messages: mockMessages,
			Index:    0,
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		var buf bytes.Buffer
		err = ConsumeLogEvents(ctx, mockReader, &buf)
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	})
}

func assertBasicLogOutput(t *testing.T, output string, timestamp time.Time, level service.LogLevel, service, message string) {
	t.Helper()

	expectedTimestampStr := timestamp.Format(time.RFC3339)
	expectedLevelStr := fmt.Sprintf("[%s]", level)

	if !strings.Contains(output, expectedTimestampStr) {
		t.Errorf("Expected timestamp %s in output, got: %s", expectedTimestampStr, output)
	}
	if !strings.Contains(output, expectedLevelStr) {
		t.Errorf("Expected log level %s in output, got: %s", expectedLevelStr, output)
	}
	if !strings.Contains(output, service) {
		t.Errorf("Expected service name %s in output, got: %s", service, output)
	}
	if !strings.Contains(output, message) {
		t.Errorf("Expected message %s in output, got: %s", message, output)
	}
}
