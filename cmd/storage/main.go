package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/bonniesimon/log-go/internal/storage"
)

func main() {
	http.HandleFunc("/v1/storage", storage.HandleCreate)
	http.HandleFunc("/v1/read", storage.HandleRead)

	fmt.Println("Storage server listening on", port())
	log.Fatal(http.ListenAndServe(address(), nil))
}

func address() string {
	addr := ":" + port()

	return addr
}

func port() string {
	port := os.Getenv("PORT")

	if port == "" {
		port = "8081"
	}

	return port
}
