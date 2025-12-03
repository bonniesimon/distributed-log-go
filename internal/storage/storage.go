package storage

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

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

func StoreHandler(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, "Invalid partition query param value", http.StatusBadRequest)
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

	for _, log := range logs {
		logPrint(log, partition)

		err = writeLogToPartition(partition, log)
		if err != nil {
			fmt.Println("Error: ", err)
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func writeLogToPartition(partition int, log LogEntry) error {
	path := filepath.Join("tmp", fmt.Sprintf("partition-%d.log", partition))

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	defer f.Close()

	enc := json.NewEncoder(f)
	return enc.Encode(log)
}

func logPrint(log LogEntry, partition int) {
	fmt.Println(
		"[INGEST]",
		"partition=", partition,
		"client_ip=", log.ClientIP,
		"received_at=", log.ReceivedAt,
		"service=", log.Service,
		"msg=", log.Message,
	)
}
