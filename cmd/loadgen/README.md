# log-go

## Usage

Run all services using overmind (or check `Procfile.dev` for manual startup):

```bash
overmind start -f Procfile.dev -D
overmind connect ingest
```

## Load Generator

Sends random log events to the ingest service.

```bash
go run cmd/loadgen/main.go [flags]

# Flags:
#   --url     Ingest URL (default: http://localhost:8080/v1/logs)
#   --batch   Logs per batch (default: 10)
#   --total   Total logs to send (default: 100)
#   --delay   Delay between batches in ms (default: 100)
```

**Examples:**

```bash
# Default: 100 logs in batches of 10
go run cmd/loadgen/main.go

# 1000 logs, batches of 50, 20ms delay
go run cmd/loadgen/main.go --total 1000 --batch 50 --delay 20

# Stress test
go run cmd/loadgen/main.go --total 10000 --batch 100 --delay 10
```

Generates logs for 4 services (`focus_allocator`, `event_creator`, `auth_service`, `notification_service`) with ~96 unique messages, weighted log levels (INFO most common), and optional labels.
