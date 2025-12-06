package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

// BaseLogDir is the base directory for partition log files.
// This can be overridden for testing.
var BaseLogDir = "tmp"

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

func HandleRead(w http.ResponseWriter, r *http.Request) {
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

	partitionFilePath := partitionLogFilePath(partition)
	logs, err := readLogFromPartition(partitionFilePath, limit)
	if err != nil {
		http.Error(w, fmt.Sprint("error reading log file: ", err), http.StatusBadRequest)
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

func HandleCreate(w http.ResponseWriter, r *http.Request) {
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

	for _, log := range logs {
		logPrint(log, partition)

		filePath := partitionLogFilePath(partition)
		err = writeLogToPartition(filePath, log)
		if err != nil {
			fmt.Println("Error: ", err)
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func writeLogToPartition(partitionFilePath string, log LogEntry) error {
	f, err := os.OpenFile(partitionFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	defer f.Close()

	enc := json.NewEncoder(f)
	return enc.Encode(log)
}

func readLogFromPartition(partitionFilePath string, limit int) ([]LogEntry, error) {
	f, err := os.Open(partitionFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	var all []LogEntry
	for scanner.Scan() {
		line := scanner.Bytes()

		if len(line) == 0 {
			continue
		}

		var log LogEntry

		if err := json.Unmarshal(line, &log); err != nil {
			continue
		}

		all = append(all, log)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if len(all) > limit {
		all = all[len(all)-limit:]
	}

	return all, nil
}

func partitionLogFilePath(partition int) string {
	return filepath.Join(BaseLogDir, fmt.Sprintf("partition-%d.log", partition))
}

func logPrint(log LogEntry, partition int) {
	fmt.Println(
		"[STORAGE/CREATE]",
		"partition=", partition,
		"client_ip=", log.ClientIP,
		"received_at=", log.ReceivedAt,
		"service=", log.Service,
		"msg=", log.Message,
	)
}
