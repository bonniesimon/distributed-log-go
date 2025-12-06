package ingest

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"strconv"
	"time"
)

const partitionCount = 4

type IncomingLogBody struct {
	Timestamp uint64            `json:"timestamp"`
	Service   string            `json:"service"`
	Level     string            `json:"level,omitempty"`
	Message   string            `json:"message"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type LogEntry struct {
	IncomingLogBody
	ReceivedAt     int64  `json:"received_at"`
	IngestedNodeId string `json:"ingested_node_id"`
	ClientIP       string `json:"client_ip"`
}

type IngestResponse struct {
	Received int `json:"received"`
}

func HandleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var incomingLogs []IncomingLogBody

	if err := json.NewDecoder(r.Body).Decode(&incomingLogs); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(incomingLogs) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	clientIP := clientIPFromRequest(r)
	enrichedLogs := make([]LogEntry, 0, len(incomingLogs))

	for _, incomingLog := range incomingLogs {
		enriched := enrich(incomingLog, clientIP)
		enrichedLogs = append(enrichedLogs, enriched)
	}

	for _, log := range enrichedLogs {
		partition := partitionForKey(log.Service)

		storageNode := StorageNode{partition: partition}

		err := storageNode.Append(log)
		if err != nil {
			fmt.Println("Error: ", err)
		}

		fmt.Println(
			"[INGEST/CREATE]",
			"client_ip=", log.ClientIP,
			"received_at=", log.ReceivedAt,
			"service=", log.Service,
			"msg=", log.Message,
			"partition=", storageNode.partition,
			"node=", storageNode.URL(),
		)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(IngestResponse{Received: len(enrichedLogs)})
}

func HandleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	service := r.URL.Query().Get("service")
	limitQuery := r.URL.Query().Get("limit")

	fmt.Printf("[INGEST/QUERY] service=%s limit=%s\n", service, limitQuery)

	limit, err := strconv.Atoi(limitQuery)
	if err != nil || limit < 0 {
		http.Error(w, "invalid limit query param value", http.StatusBadRequest)
		return
	}

	partition := partitionForKey(service)
	storageNode := StorageNode{partition: partition}

	logs, err := storageNode.Read(limit)
	if err != nil {
		http.Error(w, "Error reading from storage node", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(logs); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}

func enrich(incomingLog IncomingLogBody, clientIP string) LogEntry {
	return LogEntry{
		IncomingLogBody: incomingLog,
		ReceivedAt:      time.Now().UnixMilli(),
		IngestedNodeId:  "id-string-1",
		ClientIP:        clientIP,
	}
}

func partitionForKey(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))

	return int(h.Sum32() % partitionCount)
}

func clientIPFromRequest(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "unknown"
	}
	return ip
}
