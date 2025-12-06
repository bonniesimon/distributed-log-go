# log-go

## Usage

Run all services using overmind (or check `Procfile.dev` for manual startup):

```bash
overmind start -f Procfile.dev -D
overmind connect ingest
```

## Testing

Run all the tests

```bash
go test ./... -v
```

Run individual tests

```bash
go test ./internal/ingest/... -v
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

---

## Architecture

A distributed log ingestion system inspired by Kafka and Loki. Logs flow through an ingest layer that enriches and partitions data, then routes to storage nodes for persistence.

![Architecture Diagram](assets/architecture-diagram.png)

## Technical Details

- **Consistent Hashing**: FNV-1a (Fowler-Noll-Vo) hash function for deterministic partition assignment — O(1) lookup with uniform key distribution, ensuring logs from the same service are co-located for efficient querying
- **Horizontal Scalability**: Partition-based sharding (4 partitions across 2 nodes) enables linear write throughput scaling; adding nodes only requires partition rebalancing, not data migration
- **Append-Only Storage**: Log-structured storage with JSON-line format (newline-delimited JSON) — optimized for sequential writes, enables simple crash recovery by replaying from last valid record
- **Stateless Ingest Layer**: Ingest nodes are horizontally scalable with no coordination overhead; partition routing is computed per-request using deterministic hashing
- **Metadata Enrichment Pipeline**: Server-side enrichment adds observability fields (`received_at`, `client_ip`, `ingested_node_id`) at ingestion time, decoupling client instrumentation from storage schema
- **Zero External Dependencies**: Built entirely on Go's standard library (`net/http`, `encoding/json`, `hash/fnv`) — no frameworks, minimal attack surface, easy to audit and deploy
- **Testable Architecture**: Dependency injection via package-level variables enables isolated unit tests with `httptest` mock servers and `t.TempDir()` for filesystem isolation

## Roadmap

| Feature                                                                                   | What You Learn                                                 |
| ----------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| **Distributed Systems**                                                                   |                                                                |
| Sync/Async Replication — Forward to primary + replica, configurable write quorum          | Leader/follower semantics, durability vs latency tradeoffs     |
| Raft Consensus — Use hashicorp/raft for offset management & leader election               | Distributed coordination, state machine replication            |
| Segments + Indexes — Rotate logs (`segment-00001.log`), build offset→position index       | Storage engine internals, Kafka/Loki architecture              |
| Service Discovery — Replace hardcoded URLs with Consul/etcd/gossip (memberlist)           | Cluster membership, health checks, dynamic routing             |
| Horizontal Ingest Scaling — Stateless ingest behind Envoy/Nginx load balancer             | Production deployment patterns, load balancing                 |
| **Backend Engineering**                                                                   |                                                                |
| Backpressure — Bounded channel buffer, reject/delay on overflow, measure queue depth      | Concurrency patterns, load shedding, bounded memory            |
| Exponential Backoff + Jitter — Structured retry with deadlines per forwarding attempt     | Failure handling, reliability patterns                         |
| Ingest-Local WAL — Persist to local WAL when storage is down, background replay           | Crash recovery, exactly-once delivery, Fluentd/Vector patterns |
| Worker Pools — Fixed goroutine pool for storage forwarding with rate limiting             | Goroutines, buffered channels, context cancellation            |
| gRPC + Protobuf — Binary serialization, streaming RPCs for high-throughput path           | Service contracts, high-performance serialization              |
| **Production-Ready**                                                                      |                                                                |
| Multi-Tenancy — `org_id` field, per-tenant partitioning, quotas & rate limits             | SaaS architecture, resource isolation                          |
| Log Search — Filter by service, message contains, optional full-text via Bleve            | Inverted indexes, query engine design                          |
| Prometheus + Grafana — `ingest_requests_total`, `storage_disk_bytes`, `/metrics` endpoint | Observability, metrics design, SRE practices                   |
| Docker Compose Stack — One command spins up ingest, storage nodes, Prometheus, Grafana    | Container orchestration, developer experience                  |
| Log Viewer UI — Service dropdown, last N logs, auto-refresh                               | Full-stack delivery, product polish                            |
| CI Pipeline — Unit tests, integration tests, `go vet`, benchmarks on GitHub Actions       | Professional engineering habits                                |
