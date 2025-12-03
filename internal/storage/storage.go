package storage

import (
	"fmt"
	"net/http"
)

func StoreHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	partition := r.URL.Query().Get("partition")

	fmt.Println("partition is : ", partition)

	// get logs from the body. Decode it into a local struct
	// append the new logs to the partition based file

	fmt.Fprintf(w, "POST request received successfully!")
}
