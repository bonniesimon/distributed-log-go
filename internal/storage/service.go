package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// BaseLogDir is the base directory for partition log files.
// This can be overridden for testing.
var BaseLogDir = "tmp"

type Service struct{}

func (s *Service) Store(partition int, logs []LogEntry) error {
	for _, log := range logs {
		logPrint(log, partition)

		filePath := partitionLogFilePath(partition)
		err := writeLogToPartition(filePath, log)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) Read(partition int, limit int) ([]LogEntry, error) {
	partitionFilePath := partitionLogFilePath(partition)
	logs, err := readLogFromPartition(partitionFilePath, limit)
	if err != nil {
		return nil, err
	}

	return logs, nil
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
