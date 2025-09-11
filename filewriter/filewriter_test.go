package filewriter

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewLogFileWriter(t *testing.T) {
	basePath := "/tmp/test-logs"
	writer := NewLogFileWriter(basePath)
	
	if writer.basePath != basePath {
		t.Errorf("Expected basePath %s, got %s", basePath, writer.basePath)
	}
	
	if writer.files == nil {
		t.Error("Expected files map to be initialized")
	}
}

func TestWriteLog(t *testing.T) {
	tempDir := t.TempDir()
	writer := NewLogFileWriter(tempDir)
	defer writer.Close()
	
	testCases := []struct {
		level   string
		message string
	}{
		{"INFO", "Test info message"},
		{"ERROR", "Test error message"},
		{"WARN", "Test warning message"},
	}
	
	for _, tc := range testCases {
		err := writer.WriteLog(tc.level, tc.message)
		if err != nil {
			t.Errorf("Failed to write log: %v", err)
		}
	}
	
	// Verify files were created with correct names
	today := time.Now().Format(dateFormat)
	for _, tc := range testCases {
		expectedFile := filepath.Join(tempDir, tc.level+"_"+today+".log")
		if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
			t.Errorf("Expected file %s was not created", expectedFile)
		}
	}
}

func TestFileContent(t *testing.T) {
	tempDir := t.TempDir()
	writer := NewLogFileWriter(tempDir)
	defer writer.Close()
	
	level := "INFO"
	message := "Test message content"
	
	err := writer.WriteLog(level, message)
	if err != nil {
		t.Fatalf("Failed to write log: %v", err)
	}
	
	// Read the file and verify content
	today := time.Now().Format(dateFormat)
	filename := filepath.Join(tempDir, level+"_"+today+".log")
	
	content, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	if !strings.Contains(string(content), message) {
		t.Errorf("Expected file to contain '%s', got '%s'", message, string(content))
	}
}

func TestMultipleWritesToSameFile(t *testing.T) {
	tempDir := t.TempDir()
	writer := NewLogFileWriter(tempDir)
	defer writer.Close()
	
	level := "INFO"
	messages := []string{"First message", "Second message", "Third message"}
	
	for _, msg := range messages {
		err := writer.WriteLog(level, msg)
		if err != nil {
			t.Errorf("Failed to write log: %v", err)
		}
	}
	
	// Read the file and verify all messages are present
	today := time.Now().Format(dateFormat)
	filename := filepath.Join(tempDir, level+"_"+today+".log")
	
	content, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	for _, msg := range messages {
		if !strings.Contains(string(content), msg) {
			t.Errorf("Expected file to contain '%s'", msg)
		}
	}
}

func TestGetFilename(t *testing.T) {
	writer := NewLogFileWriter("/test/path")
	
	testTime := time.Date(2023, 12, 25, 15, 30, 0, 0, time.UTC)
	
	testCases := []struct {
		level    string
		expected string
	}{
		{"INFO", "/test/path/INFO_2023-12-25.log"},
		{"ERROR", "/test/path/ERROR_2023-12-25.log"},
		{"WARN", "/test/path/WARN_2023-12-25.log"},
	}
	
	for _, tc := range testCases {
		result := writer.getFilename(tc.level, testTime)
		if result != tc.expected {
			t.Errorf("Expected filename '%s', got '%s'", tc.expected, result)
		}
	}
}

func TestClose(t *testing.T) {
	tempDir := t.TempDir()
	writer := NewLogFileWriter(tempDir)
	
	// Write some logs to create files
	writer.WriteLog("INFO", "test message")
	writer.WriteLog("ERROR", "error message")
	
	// Close should not return an error
	err := writer.Close()
	if err != nil {
		t.Errorf("Close returned an error: %v", err)
	}
	
	// Verify files map is reset
	if len(writer.files) != 0 {
		t.Errorf("Expected files map to be empty after close, got %d entries", len(writer.files))
	}
}

func TestConcurrentWrites(t *testing.T) {
	tempDir := t.TempDir()
	writer := NewLogFileWriter(tempDir)
	defer writer.Close()
	
	done := make(chan bool)
	numGoroutines := 10
	messagesPerGoroutine := 10
	
	// Start multiple goroutines writing to the same file
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < messagesPerGoroutine; j++ {
				err := writer.WriteLog("INFO", fmt.Sprintf("Message from goroutine %d, iteration %d", id, j))
				if err != nil {
					t.Errorf("Failed to write log: %v", err)
				}
			}
			done <- true
		}(i)
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
	
	// Verify the file exists and has content
	today := time.Now().Format(dateFormat)
	filename := filepath.Join(tempDir, "INFO_"+today+".log")
	
	content, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	expectedLines := numGoroutines * messagesPerGoroutine
	
	if len(lines) != expectedLines {
		t.Errorf("Expected %d lines, got %d", expectedLines, len(lines))
	}
}