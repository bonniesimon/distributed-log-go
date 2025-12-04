package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/bonniesimon/log-go/internal/ingest"
)

func main() {
	http.HandleFunc("/v1/logs", ingest.HandleCreate)

	fmt.Println("Server listening on 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
