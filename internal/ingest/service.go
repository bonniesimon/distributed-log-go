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

func (s *Service) Ingest(logs []IncomingLogBody, clientIP string) error {
	for _, incomingLog := range logs {
		enriched := enrich(incomingLog, clientIP)

		partition := partitionForKey(enriched.Service)

		err := s.storage.Append(partition, enriched)
		if err != nil {
			fmt.Println("Error: ", err)
		}

		fmt.Println(
			"[INGEST/CREATE]",
			"client_ip=", enriched.ClientIP,
			"received_at=", enriched.ReceivedAt,
			"service=", enriched.Service,
			"msg=", enriched.Message,
			"partition=", partition,
		)
	}

	return nil
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
