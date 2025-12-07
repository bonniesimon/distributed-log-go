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

// setupHandler creates the handler with all dependencies for testing
func setupHandler() *Handler {
	service := &Service{}
	return NewHandler(service)
}

// setupTempDir creates a temp directory and overrides BaseLogDir for testing
// Returns a cleanup function that should be deferred
func setupTempDir(t *testing.T) (string, func()) {
	tmpDir := t.TempDir()
	originalBaseLogDir := BaseLogDir
	BaseLogDir = tmpDir

	cleanup := func() {
		BaseLogDir = originalBaseLogDir
	}

	return tmpDir, cleanup
}

func TestHandleCreate(t *testing.T) {
	tmpDir, cleanup := setupTempDir(t)
	defer cleanup()

	handler := setupHandler()

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

	handler.HandleCreate(w, req)

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
	_, cleanup := setupTempDir(t)
	defer cleanup()

	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/storage?partition=0", nil)
	w := httptest.NewRecorder()

	handler.HandleCreate(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", w.Code)
	}
}

func TestHandleCreate_MissingPartition(t *testing.T) {
	_, cleanup := setupTempDir(t)
	defer cleanup()

	handler := setupHandler()

	body, _ := json.Marshal([]LogEntry{{Message: "test"}})
	req := httptest.NewRequest(http.MethodPost, "/v1/storage", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.HandleCreate(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleCreate_EmptyBody(t *testing.T) {
	_, cleanup := setupTempDir(t)
	defer cleanup()

	handler := setupHandler()

	body, _ := json.Marshal([]LogEntry{})
	req := httptest.NewRequest(http.MethodPost, "/v1/storage?partition=0", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.HandleCreate(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleRead(t *testing.T) {
	tmpDir, cleanup := setupTempDir(t)
	defer cleanup()

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

	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/storage?partition=0&limit=10", nil)
	w := httptest.NewRecorder()

	handler.HandleRead(w, req)

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
	_, cleanup := setupTempDir(t)
	defer cleanup()

	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/storage?partition=abc&limit=10", nil)
	w := httptest.NewRecorder()

	handler.HandleRead(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleRead_InvalidMethod(t *testing.T) {
	_, cleanup := setupTempDir(t)
	defer cleanup()

	handler := setupHandler()

	req := httptest.NewRequest(http.MethodPost, "/v1/storage?partition=0&limit=10", nil)
	w := httptest.NewRecorder()

	handler.HandleRead(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleRead_InvalidLimit(t *testing.T) {
	_, cleanup := setupTempDir(t)
	defer cleanup()

	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/storage?partition=0&limit=abc", nil)
	w := httptest.NewRecorder()

	handler.HandleRead(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleRead_NegativePartition(t *testing.T) {
	_, cleanup := setupTempDir(t)
	defer cleanup()

	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/storage?partition=-1&limit=10", nil)
	w := httptest.NewRecorder()

	handler.HandleRead(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleRead_NegativeLimit(t *testing.T) {
	_, cleanup := setupTempDir(t)
	defer cleanup()

	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/storage?partition=0&limit=-5", nil)
	w := httptest.NewRecorder()

	handler.HandleRead(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}
