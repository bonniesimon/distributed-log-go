package ingest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type LogEntry struct {
	Timestamp uint64 `json:"timestamp"`
	Service   string `json:"service"`
	Level     string `json:"level,omitempty"`
	Message   string `json:"message"`
}

func PostHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var logs []LogEntry

	if err := json.NewDecoder(r.Body).Decode(&logs); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(logs) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	for _, entry := range logs {
		logPrint(entry, time.Now().UnixMilli())
	}

	fmt.Fprintf(w, "POST request received successfully!")
}

func logPrint(entry LogEntry, receivedAt int64) {
	fmt.Println(
		"[INGEST]",
		"received_at=", receivedAt,
		"service=", entry.Service,
		"msg=", entry.Message,
	)
}
