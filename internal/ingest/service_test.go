package ingest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPartitionForKey(t *testing.T) {
	// Same key should always return same partition
	p1 := partitionForKey("my-service")
	p2 := partitionForKey("my-service")

	if p1 != p2 {
		t.Errorf("same key should produce same partition")
	}

	// Partition should be within range
	if p1 < 0 || p1 >= partitionCount {
		t.Errorf("partition %d out of range [0, %d)", p1, partitionCount)
	}
}

func TestPartitionForKey_DifferentKeys(t *testing.T) {
	// Different keys may produce different partitions (not guaranteed, but tests distribution)
	partitions := make(map[int]bool)
	keys := []string{"service-a", "service-b", "service-c", "service-d", "service-e"}

	for _, key := range keys {
		p := partitionForKey(key)
		partitions[p] = true

		if p < 0 || p >= partitionCount {
			t.Errorf("partition %d out of range [0, %d) for key %s", p, partitionCount, key)
		}
	}
}

func TestEnrich(t *testing.T) {
	incoming := IncomingLogBody{
		Timestamp: 1234567890,
		Service:   "test-service",
		Level:     "info",
		Message:   "test message",
		Labels:    map[string]string{"env": "prod"},
	}

	enriched := enrich(incoming, "192.168.1.1")

	if enriched.Timestamp != incoming.Timestamp {
		t.Errorf("expected timestamp %d, got %d", incoming.Timestamp, enriched.Timestamp)
	}

	if enriched.Service != incoming.Service {
		t.Errorf("expected service %s, got %s", incoming.Service, enriched.Service)
	}

	if enriched.Level != incoming.Level {
		t.Errorf("expected level %s, got %s", incoming.Level, enriched.Level)
	}

	if enriched.Message != incoming.Message {
		t.Errorf("expected message %s, got %s", incoming.Message, enriched.Message)
	}

	if enriched.ClientIP != "192.168.1.1" {
		t.Errorf("expected client IP '192.168.1.1', got '%s'", enriched.ClientIP)
	}

	if enriched.ReceivedAt == 0 {
		t.Error("expected ReceivedAt to be set")
	}

	if enriched.IngestedNodeId == "" {
		t.Error("expected IngestedNodeId to be set")
	}
}

func TestServiceIngest(t *testing.T) {
	var receivedLogs []LogEntry
	mockStorage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var logs []LogEntry
		json.NewDecoder(r.Body).Decode(&logs)
		receivedLogs = append(receivedLogs, logs...)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer mockStorage.Close()

	// Point all partitions to mock server
	originalURLs := make(map[int]string)
	for k, v := range StorageNodeURLs {
		originalURLs[k] = v
		StorageNodeURLs[k] = mockStorage.URL
	}
	defer func() {
		for k, v := range originalURLs {
			StorageNodeURLs[k] = v
		}
	}()

	storage := &StorageClient{}
	service := NewService(storage)

	incomingLogs := []IncomingLogBody{
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

	err := service.Ingest(incomingLogs, "10.0.0.1")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(receivedLogs) != 2 {
		t.Fatalf("expected 2 logs sent to storage, got %d", len(receivedLogs))
	}

	if receivedLogs[0].ClientIP != "10.0.0.1" {
		t.Errorf("expected client IP '10.0.0.1', got '%s'", receivedLogs[0].ClientIP)
	}
}

func TestServiceQuery(t *testing.T) {
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

	mockStorage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(mockLogs)
	}))
	defer mockStorage.Close()

	// Point all partitions to mock server
	originalURLs := make(map[int]string)
	for k, v := range StorageNodeURLs {
		originalURLs[k] = v
		StorageNodeURLs[k] = mockStorage.URL
	}
	defer func() {
		for k, v := range originalURLs {
			StorageNodeURLs[k] = v
		}
	}()

	storage := &StorageClient{}
	service := NewService(storage)

	logs, err := service.Query("test-service", 10)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	if logs[0].Service != "test-service" {
		t.Errorf("expected service 'test-service', got '%s'", logs[0].Service)
	}
}

func TestServiceQuery_StorageError(t *testing.T) {
	mockStorage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockStorage.Close()

	// Point all partitions to mock server
	originalURLs := make(map[int]string)
	for k, v := range StorageNodeURLs {
		originalURLs[k] = v
		StorageNodeURLs[k] = mockStorage.URL
	}
	defer func() {
		for k, v := range originalURLs {
			StorageNodeURLs[k] = v
		}
	}()

	storage := &StorageClient{}
	service := NewService(storage)

	_, err := service.Query("test-service", 10)

	if err == nil {
		t.Error("expected error, got nil")
	}
}
