package ingest

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"
)

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

func PostHandler(w http.ResponseWriter, r *http.Request) {
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
		logPrint(log)
	}

	fmt.Fprintf(w, "POST request received successfully!")
}

func enrich(incomingLog IncomingLogBody, clientIP string) LogEntry {
	return LogEntry{
		IncomingLogBody: incomingLog,
		ReceivedAt:      time.Now().UnixMilli(),
		IngestedNodeId:  "id-string-1",
		ClientIP:        clientIP,
	}
}

func clientIPFromRequest(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "unknown"
	}
	return ip
}

func logPrint(log LogEntry) {
	fmt.Println(
		"[INGEST]",
		"client_ip=", log.ClientIP,
		"received_at=", log.ReceivedAt,
		"service=", log.Service,
		"msg=", log.Message,
	)
}
