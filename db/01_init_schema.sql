CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,

    task_type VARCHAR(100) NOT NULL,

    payload JSONB NOT NULL,

    status VARCHAR(20) NOT NULL DEFAULT 'pending',

    claim_token UUID,

    worker_id VARCHAR(100),

    last_heartbeat_at TIMESTAMP WITH TIME ZONE,

    attempts INTEGER NOT NULL DEFAULT 0,

    run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at
ON jobs (status, run_at);

CREATE INDEX IF NOT EXISTS idx_jobs_processing_heartbeat
ON jobs (status, last_heartbeat_at);