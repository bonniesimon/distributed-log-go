package ingest

import (
	"fmt"
	"time"
)

type Service struct {
	storage *StorageClient
}

func NewService(storage *StorageClient) *Service {
	return &Service{storage: storage}
}

func (s *Service) Ingest(logs []IncomingLogBody, clientIP string) ([]LogEntry, error) {
	enrichedLogs := make([]LogEntry, 0, len(logs))

	for _, incomingLog := range logs {
		enriched := enrich(incomingLog, clientIP)
		enrichedLogs = append(enrichedLogs, enriched)

		partition := partitionForKey(enriched.Service)

		err := s.storage.Append(partition, enriched)
		if err != nil {
			fmt.Println("Error: ", err)
		}

	}

	return enrichedLogs, nil
}

func (s *Service) Query(service string, limit int) ([]LogEntry, error) {
	partition := partitionForKey(service)
	logs, err := s.storage.Read(partition, limit)
	if err != nil {
		return nil, err
	}

	return logs, nil
}

func enrich(incomingLog IncomingLogBody, clientIP string) LogEntry {
	return LogEntry{
		IncomingLogBody: incomingLog,
		ReceivedAt:      time.Now().UnixMilli(),
		IngestedNodeId:  "id-string-1",
		ClientIP:        clientIP,
	}
}
