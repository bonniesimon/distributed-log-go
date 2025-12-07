package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestServiceStore(t *testing.T) {
	tmpDir := t.TempDir()
	originalBaseLogDir := BaseLogDir
	BaseLogDir = tmpDir
	defer func() { BaseLogDir = originalBaseLogDir }()

	service := &Service{}

	logs := []LogEntry{
		{
			Timestamp: 1234567890,
			Service:   "test-service",
			Level:     "info",
			Message:   "test message 1",
		},
		{
			Timestamp: 1234567891,
			Service:   "test-service",
			Level:     "error",
			Message:   "test message 2",
		},
	}

	err := service.Store(0, logs)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify file was created and contains logs
	filePath := filepath.Join(tmpDir, "partition-0.log")
	f, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	defer f.Close()

	var storedLogs []LogEntry
	decoder := json.NewDecoder(f)
	for decoder.More() {
		var log LogEntry
		if err := decoder.Decode(&log); err != nil {
			t.Fatalf("failed to decode log: %v", err)
		}
		storedLogs = append(storedLogs, log)
	}

	if len(storedLogs) != 2 {
		t.Errorf("expected 2 logs stored, got %d", len(storedLogs))
	}

	if storedLogs[0].Message != "test message 1" {
		t.Errorf("expected message 'test message 1', got '%s'", storedLogs[0].Message)
	}
}

func TestServiceStore_DifferentPartitions(t *testing.T) {
	tmpDir := t.TempDir()
	originalBaseLogDir := BaseLogDir
	BaseLogDir = tmpDir
	defer func() { BaseLogDir = originalBaseLogDir }()

	service := &Service{}

	logs0 := []LogEntry{{Message: "partition 0 log"}}
	logs1 := []LogEntry{{Message: "partition 1 log"}}

	err := service.Store(0, logs0)
	if err != nil {
		t.Fatalf("unexpected error storing to partition 0: %v", err)
	}

	err = service.Store(1, logs1)
	if err != nil {
		t.Fatalf("unexpected error storing to partition 1: %v", err)
	}

	// Verify both partition files exist
	if _, err := os.Stat(filepath.Join(tmpDir, "partition-0.log")); os.IsNotExist(err) {
		t.Error("expected partition-0.log to be created")
	}
	if _, err := os.Stat(filepath.Join(tmpDir, "partition-1.log")); os.IsNotExist(err) {
		t.Error("expected partition-1.log to be created")
	}
}

func TestServiceRead(t *testing.T) {
	tmpDir := t.TempDir()
	originalBaseLogDir := BaseLogDir
	BaseLogDir = tmpDir
	defer func() { BaseLogDir = originalBaseLogDir }()

	// Create test log file
	testLogs := []LogEntry{
		{Timestamp: 1234567890, Service: "test-service", Message: "message 1"},
		{Timestamp: 1234567891, Service: "test-service", Message: "message 2"},
		{Timestamp: 1234567892, Service: "test-service", Message: "message 3"},
	}

	filePath := filepath.Join(tmpDir, "partition-0.log")
	f, _ := os.Create(filePath)
	encoder := json.NewEncoder(f)
	for _, log := range testLogs {
		encoder.Encode(log)
	}
	f.Close()

	service := &Service{}

	logs, err := service.Read(0, 10)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(logs) != 3 {
		t.Errorf("expected 3 logs, got %d", len(logs))
	}
}

func TestServiceRead_WithLimit(t *testing.T) {
	tmpDir := t.TempDir()
	originalBaseLogDir := BaseLogDir
	BaseLogDir = tmpDir
	defer func() { BaseLogDir = originalBaseLogDir }()

	// Create test log file with 5 entries
	filePath := filepath.Join(tmpDir, "partition-0.log")
	f, _ := os.Create(filePath)
	encoder := json.NewEncoder(f)
	for i := 0; i < 5; i++ {
		encoder.Encode(LogEntry{Message: "message"})
	}
	f.Close()

	service := &Service{}

	logs, err := service.Read(0, 2)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should return last 2 logs
	if len(logs) != 2 {
		t.Errorf("expected 2 logs (limited), got %d", len(logs))
	}
}

func TestServiceRead_FileNotExists(t *testing.T) {
	tmpDir := t.TempDir()
	originalBaseLogDir := BaseLogDir
	BaseLogDir = tmpDir
	defer func() { BaseLogDir = originalBaseLogDir }()

	service := &Service{}

	_, err := service.Read(99, 10)

	if err == nil {
		t.Error("expected error for non-existent partition file, got nil")
	}
}
