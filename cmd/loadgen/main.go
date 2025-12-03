package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

type LogEntry struct {
	Timestamp uint64            `json:"timestamp"`
	Service   string            `json:"service"`
	Level     string            `json:"level"`
	Message   string            `json:"message"`
	Labels    map[string]string `json:"labels,omitempty"`
}

var services = []string{
	"focus_allocator",
	"event_creator",
	"auth_service",
	"notification_service",
}

var levels = []string{"DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"}

var messagesByService = map[string][]string{
	"focus_allocator": {
		"focus time allocated",
		"focus block created for user",
		"focus session started",
		"focus session completed",
		"focus session interrupted",
		"focus time extended by user",
		"focus time shortened by user",
		"calculating optimal focus windows",
		"focus preferences loaded",
		"focus preferences updated",
		"daily focus quota reached",
		"weekly focus summary generated",
		"focus block conflict detected",
		"resolving overlapping focus blocks",
		"focus time suggestion generated",
		"auto-allocation triggered",
		"focus buffer time added",
		"deep work block scheduled",
		"shallow work block scheduled",
		"focus allocation failed - no available slots",
		"recalculating focus distribution",
		"focus streak milestone reached",
		"focus analytics computed",
		"optimal productivity window identified",
	},
	"event_creator": {
		"focus time event created",
		"calendar event created",
		"event updated successfully",
		"event deleted",
		"recurring event generated",
		"event reminder set",
		"event conflict detected",
		"event rescheduled",
		"event attendees notified",
		"event location updated",
		"event duration modified",
		"all-day event created",
		"event sync initiated",
		"event sync completed",
		"external calendar event imported",
		"event export requested",
		"meeting event created",
		"break event inserted",
		"buffer event added between meetings",
		"event validation passed",
		"event validation failed - invalid time range",
		"batch events created",
		"event template applied",
		"event color label assigned",
	},
	"auth_service": {
		"user login attempt",
		"user login successful",
		"user login failed - invalid credentials",
		"user login failed - account locked",
		"password reset requested",
		"password reset completed",
		"token generated successfully",
		"token validation successful",
		"token expired",
		"token revoked",
		"refresh token issued",
		"MFA challenge sent",
		"MFA verification successful",
		"MFA verification failed",
		"session created",
		"session invalidated",
		"API key validated",
		"API key rejected - expired",
		"OAuth callback received",
		"OAuth token exchanged",
		"user permissions checked",
		"access denied - insufficient permissions",
		"account lockout warning",
		"suspicious login detected",
	},
	"notification_service": {
		"email notification queued",
		"email sent successfully",
		"email delivery failed",
		"email bounced",
		"push notification sent",
		"push notification failed - invalid token",
		"SMS notification queued",
		"SMS sent successfully",
		"SMS delivery failed",
		"focus reminder sent",
		"event reminder delivered",
		"daily summary email sent",
		"weekly digest prepared",
		"notification template rendered",
		"notification preferences checked",
		"user unsubscribed from notifications",
		"bulk notification job started",
		"bulk notification job completed",
		"notification rate limit applied",
		"in-app notification created",
		"notification channel selected",
		"quiet hours respected - notification delayed",
		"urgent notification override applied",
		"notification delivery confirmed",
	},
}

func main() {
	url := flag.String("url", "http://localhost:8080/v1/logs", "Ingest URL")
	batchSize := flag.Int("batch", 10, "Logs per batch")
	total := flag.Int("total", 100, "Total logs to send")
	delay := flag.Int("delay", 100, "Delay between batches in milliseconds")
	flag.Parse()

	fmt.Printf("🚀 Log Generator Starting\n")
	fmt.Printf("   URL: %s\n", *url)
	fmt.Printf("   Batch Size: %d\n", *batchSize)
	fmt.Printf("   Total Logs: %d\n", *total)
	fmt.Printf("   Delay: %dms\n\n", *delay)

	timestamp := uint64(time.Now().UnixMilli())
	sent := 0
	batchNum := 0

	for sent < *total {
		batchNum++
		remaining := *total - sent
		currentBatchSize := *batchSize
		if remaining < currentBatchSize {
			currentBatchSize = remaining
		}

		logs := make([]LogEntry, 0, currentBatchSize)
		for i := 0; i < currentBatchSize; i++ {
			log := generateLog(&timestamp)
			logs = append(logs, log)
		}

		err := sendBatch(*url, logs)
		if err != nil {
			fmt.Printf("❌ Batch %d failed: %v\n", batchNum, err)
		} else {
			fmt.Printf("✅ Batch %d sent (%d logs, total: %d/%d)\n", batchNum, currentBatchSize, sent+currentBatchSize, *total)
		}

		sent += currentBatchSize

		if sent < *total {
			time.Sleep(time.Duration(*delay) * time.Millisecond)
		}
	}

	fmt.Printf("\n🎉 Done! Sent %d logs in %d batches\n", sent, batchNum)
}

func generateLog(timestamp *uint64) LogEntry {
	service := services[rand.Intn(len(services))]
	level := levels[rand.Intn(len(levels))]
	messages := messagesByService[service]
	message := messages[rand.Intn(len(messages))]

	// Advance timestamp by 50-500ms
	*timestamp += uint64(50 + rand.Intn(450))

	log := LogEntry{
		Timestamp: *timestamp,
		Service:   service,
		Level:     level,
		Message:   message,
	}

	// Occasionally add labels (30% chance)
	if rand.Float32() < 0.3 {
		log.Labels = generateLabels(service)
	}

	return log
}

func generateLabels(service string) map[string]string {
	labels := make(map[string]string)

	switch service {
	case "focus_allocator":
		labels["focus_type"] = randomChoice([]string{"deep_work", "shallow_work", "break", "meeting"})
		labels["duration_minutes"] = randomChoice([]string{"25", "50", "90", "120"})
	case "event_creator":
		labels["event_type"] = randomChoice([]string{"focus_block", "meeting", "break", "reminder"})
		labels["calendar"] = randomChoice([]string{"primary", "work", "personal"})
	case "auth_service":
		labels["auth_method"] = randomChoice([]string{"password", "oauth", "api_key", "mfa"})
	case "notification_service":
		labels["channel"] = randomChoice([]string{"email", "sms", "push", "in_app"})
	}

	return labels
}

func randomChoice(choices []string) string {
	return choices[rand.Intn(len(choices))]
}

func sendBatch(url string, logs []LogEntry) error {
	payload, err := json.Marshal(logs)
	if err != nil {
		return fmt.Errorf("failed to marshal logs: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("server returned %d", resp.StatusCode)
	}

	return nil
}
