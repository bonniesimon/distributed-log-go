package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/bonniesimon/log-go/internal/ingest"
)

func main() {
	storage := &ingest.StorageClient{}
	service := ingest.NewService(storage)
	handler := ingest.NewHandler(service)

	http.HandleFunc("/v1/logs", handler.HandleCreate)
	http.HandleFunc("/v1/query", handler.HandleQuery)

	fmt.Println("Server listening on 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
