package storage

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHandleCreate(t *testing.T) {
	// Setup temp directory
	tmpDir := t.TempDir()
	originalBaseLogDir := BaseLogDir
	BaseLogDir = tmpDir
	defer func() { BaseLogDir = originalBaseLogDir }()

	logs := []LogEntry{
		{
			Timestamp: 1234567890,
			Service:   "test-service",
			Level:     "info",
			Message:   "test message",
		},
	}

	body, _ := json.Marshal(logs)
	req := httptest.NewRequest(http.MethodPost, "/v1/storage?partition=0", bytes.NewReader(body))
	w := httptest.NewRecorder()

	HandleCreate(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	// Verify file was created
	filePath := filepath.Join(tmpDir, "partition-0.log")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("expected partition file to be created")
	}
}

func TestHandleCreate_InvalidMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/storage?partition=0", nil)
	w := httptest.NewRecorder()

	HandleCreate(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", w.Code)
	}
}

func TestHandleCreate_MissingPartition(t *testing.T) {
	body, _ := json.Marshal([]LogEntry{{Message: "test"}})
	req := httptest.NewRequest(http.MethodPost, "/v1/storage", bytes.NewReader(body))
	w := httptest.NewRecorder()

	HandleCreate(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleCreate_EmptyBody(t *testing.T) {
	body, _ := json.Marshal([]LogEntry{})
	req := httptest.NewRequest(http.MethodPost, "/v1/storage?partition=0", bytes.NewReader(body))
	w := httptest.NewRecorder()

	HandleCreate(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleRead(t *testing.T) {
	// Setup temp directory with test data
	tmpDir := t.TempDir()
	originalBaseLogDir := BaseLogDir
	BaseLogDir = tmpDir
	defer func() { BaseLogDir = originalBaseLogDir }()

	// Create test log file
	testLog := LogEntry{
		Timestamp: 1234567890,
		Service:   "test-service",
		Message:   "test message",
	}
	filePath := filepath.Join(tmpDir, "partition-0.log")
	f, _ := os.Create(filePath)
	json.NewEncoder(f).Encode(testLog)
	f.Close()

	req := httptest.NewRequest(http.MethodGet, "/v1/storage?partition=0&limit=10", nil)
	w := httptest.NewRecorder()

	HandleRead(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var logs []LogEntry
	json.NewDecoder(w.Body).Decode(&logs)

	if len(logs) != 1 {
		t.Errorf("expected 1 log, got %d", len(logs))
	}
	if logs[0].Service != "test-service" {
		t.Errorf("expected service 'test-service', got '%s'", logs[0].Service)
	}
}

func TestHandleRead_InvalidPartition(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/storage?partition=abc&limit=10", nil)
	w := httptest.NewRecorder()

	HandleRead(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleRead_InvalidMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/storage?partition=0&limit=10", nil)
	w := httptest.NewRecorder()

	HandleRead(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}
