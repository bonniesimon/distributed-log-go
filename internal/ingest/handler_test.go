package ingest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// setupHandler creates the handler with all dependencies for testing
func setupHandler() *Handler {
	storage := NewStorageClient()
	service := NewService(storage)
	return NewHandler(service)
}

// setupMockStorage creates a mock storage server and overrides StorageNodeURLs
// Returns a cleanup function that should be deferred
func setupMockStorage(handler http.HandlerFunc) (*httptest.Server, func()) {
	mockStorage := httptest.NewServer(handler)

	originalURLs := make(map[int]string)
	for k, v := range StorageNodeURLs {
		originalURLs[k] = v
		StorageNodeURLs[k] = mockStorage.URL
	}

	cleanup := func() {
		mockStorage.Close()
		for k, v := range originalURLs {
			StorageNodeURLs[k] = v
		}
	}

	return mockStorage, cleanup
}

func TestHandleCreate(t *testing.T) {
	var receivedLogs []LogEntry
	var mu sync.Mutex
	_, cleanup := setupMockStorage(func(w http.ResponseWriter, r *http.Request) {
		var logs []LogEntry
		json.NewDecoder(r.Body).Decode(&logs)
		mu.Lock()
		receivedLogs = append(receivedLogs, logs...)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	defer cleanup()

	handler := setupHandler()

	logs := []IncomingLogBody{
		{
			Timestamp: 1234567890,
			Service:   "test-service",
			Level:     "info",
			Message:   "test message",
		},
	}

	body, _ := json.Marshal(logs)
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	req.RemoteAddr = "192.168.1.1:12345"
	w := httptest.NewRecorder()

	handler.HandleCreate(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var response IngestResponse
	json.NewDecoder(w.Body).Decode(&response)

	if response.Received != 1 {
		t.Errorf("expected received=1, got %d", response.Received)
	}

	if len(receivedLogs) != 1 {
		t.Fatalf("expected 1 log sent to storage, got %d", len(receivedLogs))
	}

	if receivedLogs[0].Service != "test-service" {
		t.Errorf("expected service 'test-service', got '%s'", receivedLogs[0].Service)
	}

	if receivedLogs[0].ClientIP != "192.168.1.1" {
		t.Errorf("expected client IP '192.168.1.1', got '%s'", receivedLogs[0].ClientIP)
	}
}

func TestHandleCreate_InvalidMethod(t *testing.T) {
	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/ingest", nil)
	w := httptest.NewRecorder()

	handler.HandleCreate(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", w.Code)
	}
}

func TestHandleCreate_EmptyBody(t *testing.T) {
	handler := setupHandler()

	body, _ := json.Marshal([]IncomingLogBody{})
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.HandleCreate(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleCreate_InvalidJSON(t *testing.T) {
	handler := setupHandler()

	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader([]byte("not json")))
	w := httptest.NewRecorder()

	handler.HandleCreate(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleQuery(t *testing.T) {
	mockLogs := []LogEntry{
		{
			IncomingLogBody: IncomingLogBody{
				Timestamp: 1234567890,
				Service:   "test-service",
				Level:     "info",
				Message:   "test message",
			},
			ReceivedAt:     1234567890000,
			IngestedNodeId: "node-1",
			ClientIP:       "192.168.1.1",
		},
	}

	_, cleanup := setupMockStorage(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(mockLogs)
	})
	defer cleanup()

	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/query?service=test-service&limit=10", nil)
	w := httptest.NewRecorder()

	handler.HandleQuery(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var responseLogs []LogEntry
	json.NewDecoder(w.Body).Decode(&responseLogs)

	if len(responseLogs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(responseLogs))
	}

	if responseLogs[0].Service != "test-service" {
		t.Errorf("expected service 'test-service', got '%s'", responseLogs[0].Service)
	}

	if responseLogs[0].Message != "test message" {
		t.Errorf("expected message 'test message', got '%s'", responseLogs[0].Message)
	}
}

func TestHandleQuery_InvalidMethod(t *testing.T) {
	handler := setupHandler()

	req := httptest.NewRequest(http.MethodPost, "/v1/query?service=test&limit=10", nil)
	w := httptest.NewRecorder()

	handler.HandleQuery(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", w.Code)
	}
}

func TestHandleQuery_InvalidLimit(t *testing.T) {
	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/query?service=test&limit=abc", nil)
	w := httptest.NewRecorder()

	handler.HandleQuery(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleQuery_NegativeLimit(t *testing.T) {
	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/query?service=test&limit=-5", nil)
	w := httptest.NewRecorder()

	handler.HandleQuery(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleQuery_MissingLimit(t *testing.T) {
	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/query?service=test", nil)
	w := httptest.NewRecorder()

	handler.HandleQuery(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleQuery_StorageError(t *testing.T) {
	_, cleanup := setupMockStorage(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer cleanup()

	handler := setupHandler()

	req := httptest.NewRequest(http.MethodGet, "/v1/query?service=test&limit=10", nil)
	w := httptest.NewRecorder()

	handler.HandleQuery(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}
