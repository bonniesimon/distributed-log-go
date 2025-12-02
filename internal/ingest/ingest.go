package ingest

import (
	"fmt"
	"io"
	"net/http"
)

func PostHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)

	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	fmt.Printf("Received POST request with body: %s\n", body)
	fmt.Fprintf(w, "POST request received successfully!")
}
