# Distributed Job Queue

A production-ready distributed job queue built on **PostgreSQL** (durable storage & coordination) and **Redis** (fast dispatch). It implements the **Transactional Outbox pattern** to guarantee at-least-once delivery without losing jobs, even across crashes and network partitions.

---

## Architecture

```
┌──────────────┐     INSERT + COMMIT     ┌─────────────────────────────┐
│   Producer   │ ──────────────────────► │         PostgreSQL           │
│ (producer.py)│                         │  jobs table (source of truth)│
└──────────────┘                         └─────────────┬───────────────┘
                                                       │ Outbox Relay
                                                       │ (sweeper.py)
                                                       ▼
                                         ┌─────────────────────────────┐
                                         │     Redis Sorted Set         │
                                         │       "jobs_queue"           │
                                         └─────────────┬───────────────┘
                                                       │ ZPOPMIN
                                                       ▼
                                         ┌─────────────────────────────┐
                                         │    Worker (worker.py)        │
                                         │  CAS claim → process → ack   │
                                         └─────────────────────────────┘
```

### Components

| Component | File | Role |
|-----------|------|------|
| **Producer** | [`src/producer.py`](src/producer.py) | Writes job to PostgreSQL, then pushes ID to Redis after commit |
| **Worker** | [`src/worker.py`](src/worker.py) | Pops job ID from Redis, claims it in PostgreSQL via CAS, processes it |
| **Sweeper** | [`src/sweeper.py`](src/sweeper.py) | Recovers zombie jobs and relays stranded outbox jobs to Redis |
| **Outbox Relay** | [`src/outbox_relay.py`](src/outbox_relay.py) | Standalone outbox relay loop (replicated inside sweeper) |
| **Schema** | [`db/01_init_schema.sql`](db/01_init_schema.sql) | PostgreSQL table & indexes |

---

## Key Design Decisions

### 1. Transactional Outbox Pattern
The producer writes the job to PostgreSQL and **commits first**, then pushes the job ID to Redis and marks `is_pushed = TRUE`. If the process crashes between commit and push, the **Sweeper's outbox relay** detects `is_pushed = FALSE` jobs and re-pushes them — ensuring no job is silently dropped.

### 2. Compare-And-Swap (CAS) Claim
Workers claim jobs with an atomic `UPDATE … WHERE status = 'pending' … RETURNING`. PostgreSQL row-level locking (`FOR UPDATE SKIP LOCKED`) prevents two workers from claiming the same job simultaneously.

### 3. Heartbeats & Zombie Recovery
While processing, each worker runs a background **heartbeat thread** that updates `last_heartbeat_at` every 5 seconds. The Sweeper scans for jobs whose heartbeat is older than `timeout_seconds` (default: 120 s) and resets them to `pending` so another worker can pick them up.

### 4. Fencing Tokens
Every claim generates a unique `claim_token` (UUID). All write-back operations (`complete_job`, `fail_job`, `heartbeat_job`) require the token to match in the `WHERE` clause. A stale worker that wakes up after losing ownership will have a mismatched token and its update will silently no-op.

### 5. Retry with Exponential Backoff
On failure, `fail_job` increments `attempts` and sets `run_at = NOW() + (attempts² × 10 seconds)`. After **5 attempts** the job is moved to `status = 'dlq'` (Dead-Letter Queue).

### 6. Delayed / Scheduled Jobs
`run_at` is a first-class column. Redis scores are set to `EXTRACT(EPOCH FROM run_at)`. Workers only pop jobs whose score ≤ `time.time()`, so future-dated jobs remain invisible until their scheduled time.

---

## Database Schema

```sql
CREATE TABLE jobs (
    id                UUID PRIMARY KEY,
    task_type         VARCHAR(100)  NOT NULL,
    payload           JSONB         NOT NULL,
    status            VARCHAR(20)   NOT NULL DEFAULT 'pending',
    claim_token       UUID,
    worker_id         VARCHAR(100),
    last_heartbeat_at TIMESTAMPTZ,
    attempts          INTEGER       NOT NULL DEFAULT 0,
    run_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    is_pushed         BOOLEAN       NOT NULL DEFAULT FALSE
);
```

### Status Lifecycle

```
pending ──► processing ──► completed
   ▲              │
   │   (retry)   │ (fail)
   └─────────────┘
                 │ (max retries)
                 ▼
               dlq
```

### Indexes

| Index | Purpose |
|-------|---------|
| `idx_jobs_status_run_at` | Fast pending-job lookup by `(status, run_at)` |
| `idx_jobs_processing_heartbeat` | Fast zombie scan by `(status, last_heartbeat_at)` |

---

## Prerequisites

| Dependency | Version |
|-----------|---------|
| Python | 3.9+ |
| PostgreSQL | 13+ |
| Redis | 6+ |

### Python Dependencies

```
psycopg2
redis
```

Install with:

```bash
pip install psycopg2-binary redis
```

---

## Configuration

Set the `DB_DSN` environment variable before running any component:

```bash
export DB_DSN="postgresql://postgres:postgres@127.0.0.1:5432/postgres"
```

Or create a `.env` file (loaded manually or via `python-dotenv`).

Redis defaults to `localhost:6379` db 0 and is currently hardcoded in each module.

---

## Quick Start

### 1. Apply the Schema

```bash
psql $DB_DSN -f db/01_init_schema.sql
```

### 2. Run the Demo

```bash
python -m tests.test_run
```

This script will:
1. Create the schema
2. Spin up a Worker thread + a Sweeper thread
3. Enqueue two sample jobs (`SEND_EMAIL`, `PROCESS_PAYMENT`)
4. Let them process for 30 seconds, then exit

---

## Running Components Separately

```python
import psycopg2
from src.producer import enqueue_job
from src.worker  import run_redis_worker_loop
from src.sweeper import run_sweeper_loop

conn = psycopg2.connect(DB_DSN)

# Enqueue a job
job_id = enqueue_job(conn, "SEND_EMAIL", {"to": "user@example.com"})

# Start a worker (blocking loop)
run_redis_worker_loop(conn, worker_id="Worker-1")

# Start the sweeper (blocking loop — run in a separate thread/process)
run_sweeper_loop(conn)
```

---

## Testing

### System Invariant Tests (pure SQL, no running services needed)

```bash
python -m tests.test_system
```

Covers: enqueue, `run_at` enforcement, concurrent claiming, heartbeat, zombie recovery, fencing tokens, retry/backoff, DLQ, stale-worker rejection, and idempotency notes.

### Integration Tests (requires live PostgreSQL + Redis)

```bash
export DB_DSN="postgresql://postgres:postgres@127.0.0.1:5432/postgres"
python -m tests.test_redis_system
```

Covers: Redis connectivity, schema validation, producer → Redis pipeline, outbox relay, CAS claim, concurrent workers, heartbeat, fencing, retry, DLQ, zombie recovery, and Redis duplicate protection.

---

## Guarantees & Trade-offs

| Property | Guarantee |
|----------|-----------|
| **Durability** | Jobs survive producer crashes (committed to PostgreSQL before Redis push) |
| **Delivery** | At-least-once (outbox relay + zombie recovery) |
| **Duplicate execution** | Possible — handlers should be idempotent |
| **Ordering** | Approximate FIFO within the same `run_at` bucket (Redis sorted set by score) |
| **Mutual exclusion** | PostgreSQL CAS prevents double-claiming |
| **Stale workers** | Fencing tokens prevent split-brain writes |

---

## Project Structure

```
.
├── db/
│   └── 01_init_schema.sql   # PostgreSQL DDL
├── src/
│   ├── producer.py          # Job enqueue logic
│   ├── worker.py            # Worker loop (Redis-driven)
│   ├── sweeper.py           # Zombie recovery + outbox relay loop
│   └── outbox_relay.py      # Standalone outbox relay
└── tests/
    ├── test_run.py          # End-to-end demo script
    ├── test_system.py       # SQL-level invariant tests
    └── test_redis_system.py # Full integration tests
```
