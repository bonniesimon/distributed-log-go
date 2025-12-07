package storage

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type Handler struct {
	service *Service
}

func NewHandler(s *Service) *Handler {
	return &Handler{service: s}
}

type LogEntry struct {
	Timestamp      uint64            `json:"timestamp"`
	Service        string            `json:"service"`
	Level          string            `json:"level,omitempty"`
	Message        string            `json:"message"`
	Labels         map[string]string `json:"labels,omitempty"`
	ReceivedAt     int64             `json:"received_at"`
	IngestedNodeId string            `json:"ingested_node_id"`
	ClientIP       string            `json:"client_ip"`
}

func (h *Handler) HandleRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusBadRequest)
		return
	}

	partitionQuery := r.URL.Query().Get("partition")
	limitQuery := r.URL.Query().Get("limit")

	partition, err := strconv.Atoi(partitionQuery)
	if err != nil || partition < 0 {
		http.Error(w, "invalid partition query param value", http.StatusBadRequest)
		return
	}
	limit, err := strconv.Atoi(limitQuery)
	if err != nil || limit < 0 {
		http.Error(w, "invalid limit query param value", http.StatusBadRequest)
		return
	}

	logs, err := h.service.Read(partition, limit)
	if err != nil {
		http.Error(w, fmt.Sprint("error reading from storage file", err), http.StatusBadRequest)
		return
	}

	fmt.Println(
		"[STORAGE/READ]",
		"partition=", partition,
		"limit=", limit,
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(logs); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (h *Handler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	partitionStr := r.URL.Query().Get("partition")
	if partitionStr == "" {
		http.Error(w, "Partition query param not found", http.StatusBadRequest)
		return
	}

	partition, err := strconv.Atoi(partitionStr)
	if err != nil || partition < 0 {
		http.Error(w, "invalid partition query param value", http.StatusBadRequest)
		return
	}

	var logs []LogEntry

	err = json.NewDecoder(r.Body).Decode(&logs)
	if err != nil {
		http.Error(w, "Failed to decode body", http.StatusBadRequest)
		return
	}

	if len(logs) == 0 {
		http.Error(w, "empty array", http.StatusBadRequest)
		return
	}

	err = h.service.Store(partition, logs)
	if err != nil {
		http.Error(w, "storing logs failed", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
