package ingest

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Service struct {
	storage *StorageClient
}

func NewService(storage *StorageClient) *Service {
	return &Service{storage: storage}
}

func (s *Service) Ingest(logs []IncomingLogBody, clientIP string) error {
	partitionedLogs := make(map[int][]LogEntry)

	var errs []error

	for _, incomingLog := range logs {
		enriched := enrich(incomingLog, clientIP)

		partition := partitionForKey(enriched.Service)
		partitionedLogs[partition] = append(partitionedLogs[partition], enriched)
	}

	var wg sync.WaitGroup
	errChannel := make(chan error, len(partitionedLogs))

	for partition, logs := range partitionedLogs {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := s.storage.Append(partition, logs)
			if err != nil {
				errChannel <- fmt.Errorf("failed to append to partition %d: %w", partition, err)
			}
		}()

		logsPrint(partition, logs)
	}

	wg.Wait()
	close(errChannel)

	for err := range errChannel {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
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

func logsPrint(partition int, logs []LogEntry) {
	for _, log := range logs {
		fmt.Println(
			"[INGEST/CREATE]",
			"client_ip=", log.ClientIP,
			"received_at=", log.ReceivedAt,
			"service=", log.Service,
			"msg=", log.Message,
			"partition=", partition,
		)
	}
}
