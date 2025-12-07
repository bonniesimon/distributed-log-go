package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// StorageNodeURLs maps partition numbers to storage node URLs.
// This can be overridden for testing or configuration.
var StorageNodeURLs = map[int]string{
	0: "http://localhost:8081",
	1: "http://localhost:8081",
	2: "http://localhost:8082",
	3: "http://localhost:8082",
}

type StorageClient struct {
	client *http.Client
}

func NewStorageClient() *StorageClient {
	return &StorageClient{
		client: &http.Client{},
	}
}

func (node *StorageClient) Append(partition int, logs []LogEntry) error {
	payload, err := json.Marshal(logs)
	if err != nil {
		return err
	}

	url := node.URL(partition) + "/v1/storage?partition=" + strconv.Itoa(partition)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	response, err := node.client.Do(req)
	if err != nil {
		return err
	}

	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("storage returned %d", response.StatusCode)
	}

	return nil
}

func (node *StorageClient) Read(partition int, limit int) ([]LogEntry, error) {
	if limit < 0 {
		return nil, fmt.Errorf("invalid value for limit query param")
	}

	url := node.URL(partition) + "/v1/read?partition=" + strconv.Itoa(partition) + "&limit=" + strconv.Itoa(limit)
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("storage returned %d", response.StatusCode)
	}

	var logs []LogEntry

	err = json.NewDecoder(response.Body).Decode(&logs)
	if err != nil {
		return nil, err
	}

	return logs, nil
}

func (node StorageClient) URL(partition int) string {
	if url, ok := StorageNodeURLs[partition]; ok {
		return url
	}
	return "http://localhost:8081"
}
