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
	partition int
}

func (node *StorageClient) Append(partition int, log LogEntry) error {
	node.partition = partition

	payload, err := json.Marshal([]LogEntry{log})
	if err != nil {
		return err
	}

	url := node.URL() + "/v1/storage?partition=" + strconv.Itoa(node.partition)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	// TODO: Creating &http.Client{} on every Append() call is inefficient. Consider a package-level client:
	// Can move http.Client to StorageNode stuct.
	// Or I can simply use the http.Post thingy
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

func (node *StorageClient) Read(partition int, limit int) ([]LogEntry, error) {
	node.partition = partition

	if limit < 0 {
		return nil, fmt.Errorf("invalid value for limit query param")
	}

	url := node.URL() + "/v1/read?partition=" + strconv.Itoa(node.partition) + "&limit=" + strconv.Itoa(limit)
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

func (node StorageClient) URL() string {
	if url, ok := StorageNodeURLs[node.partition]; ok {
		return url
	}
	return "http://localhost:8081"
}
