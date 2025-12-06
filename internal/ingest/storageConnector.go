package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type StorageNode struct {
	URL       string
	partition int
}

func (node StorageNode) append(log LogEntry) error {
	payload, err := json.Marshal([]LogEntry{log})
	if err != nil {
		return err
	}

	url := node.URL + "/v1/storage?partition=" + strconv.Itoa(node.partition)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}

	response, err := client.Do(req)
	if err != nil {
		return err
	}

	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("storage returned %d", response.StatusCode)
	}

	return nil
}

func (node StorageNode) read(limit int) ([]LogEntry, error) {
	if limit < 0 {
		return nil, fmt.Errorf("Invalid value for limit query param")
	}

	url := node.URL + "/v1/read?partition=" + strconv.Itoa(node.partition) + "&limit=" + strconv.Itoa(limit)
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	var logs []LogEntry

	err = json.NewDecoder(response.Body).Decode(&logs)
	if err != nil {
		return nil, err
	}

	return logs, nil
}
