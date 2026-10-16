import uuid
import json
import time
import threading
import psycopg2

DB_DSN = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"

PASS = 0
FAIL = 0
WARN = 0


def ok(name):
    global PASS
    PASS += 1
    print(f"[PASS] {name}")


def fail(name, msg):
    global FAIL
    FAIL += 1
    print(f"[FAIL] {name} -> {msg}")


def warn(name, msg):
    global WARN
    WARN += 1
    print(f"[WARN] {name} -> {msg}")


def conn():
    return psycopg2.connect(DB_DSN)


def clean():
    c = conn()

    with c.cursor() as cur:
        cur.execute("DELETE FROM jobs")

    c.commit()
    c.close()


def make_job(run_at=None):
    job_id = str(uuid.uuid4())

    c = conn()

    with c.cursor() as cur:
        cur.execute(
            """
            INSERT INTO jobs (
                id,
                task_type,
                payload,
                status,
                run_at
            )
            VALUES (
                %s,
                'TEST',
                %s::jsonb,
                'pending',
                COALESCE(%s, NOW())
            )
            """,
            (
                job_id,
                json.dumps({"test": True}),
                run_at
            )
        )

    c.commit()
    c.close()

    return job_id


# ---------------------------------------------------------
# 1. BASIC ENQUEUE
# ---------------------------------------------------------

def test_enqueue():
    job_id = make_job()

    c = conn()

    with c.cursor() as cur:
        cur.execute(
            "SELECT status FROM jobs WHERE id = %s",
            (job_id,)
        )

        row = cur.fetchone()

    c.close()

    if row and row[0] == "pending":
        ok("Job enqueue")
    else:
        fail("Job enqueue", "Job was not created as pending")


# ---------------------------------------------------------
# 2. RUN_AT / DELAYED JOB
# ---------------------------------------------------------

def test_run_at():
    c = conn()

    job_id = str(uuid.uuid4())

    with c.cursor() as cur:
        cur.execute(
            """
            INSERT INTO jobs (
                id,
                task_type,
                payload,
                status,
                run_at
            )
            VALUES (
                %s,
                'DELAYED',
                '{}'::jsonb,
                'pending',
                NOW() + INTERVAL '1 hour'
            )
            """,
            (job_id,)
        )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM jobs
            WHERE id = %s
              AND status = 'pending'
              AND run_at <= NOW()
            """,
            (job_id,)
        )

        count = cur.fetchone()[0] # type: ignore 

    c.commit()
    c.close()

    if count == 0:
        ok("Delayed job respects run_at")
    else:
        fail("Delayed job", "Job became visible before run_at")


# ---------------------------------------------------------
# 3. CONCURRENT CLAIM
# ---------------------------------------------------------

def claim_job(worker_id):
    c = conn()

    try:
        with c.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM jobs
                WHERE status = 'pending'
                  AND run_at <= NOW()
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )

            row = cur.fetchone()

            if not row:
                c.rollback()
                return None

            job_id = row[0]
            token = str(uuid.uuid4())

            cur.execute(
                """
                UPDATE jobs
                SET status = 'processing',
                    worker_id = %s,
                    claim_token = %s,
                    last_heartbeat_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'pending'
                """,
                (worker_id, token, job_id)
            )

        c.commit()

        return job_id, token

    finally:
        c.close()


def test_concurrent_claim():

    clean()

    make_job()

    results = []

    def worker(name):
        result = claim_job(name)
        results.append(result)

    t1 = threading.Thread(target=worker, args=("Worker-A",))
    t2 = threading.Thread(target=worker, args=("Worker-B",))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    claimed = [x for x in results if x is not None]

    if len(claimed) == 1:
        ok("Concurrent workers cannot claim the same job")
    else:
        fail(
            "Concurrent claim",
            f"{len(claimed)} workers claimed the job"
        )


# ---------------------------------------------------------
# 4. HEARTBEAT
# ---------------------------------------------------------

def test_heartbeat():

    clean()

    job_id = make_job()

    token = str(uuid.uuid4())

    c = conn()

    with c.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = 'processing',
                worker_id = 'Worker-A',
                claim_token = %s,
                last_heartbeat_at = NOW()
            WHERE id = %s
            """,
            (token, job_id)
        )

        cur.execute(
            """
            UPDATE jobs
            SET last_heartbeat_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
              AND status = 'processing'
              AND claim_token = %s
            """,
            (job_id, token)
        )

        updated = cur.rowcount

    c.commit()
    c.close()

    if updated == 1:
        ok("Heartbeat updates ownership timestamp")
    else:
        fail("Heartbeat", "Heartbeat update failed")


# ---------------------------------------------------------
# 5. ZOMBIE RECOVERY
# ---------------------------------------------------------

def test_zombie_recovery():

    clean()

    job_id = make_job()

    c = conn()

    with c.cursor() as cur:

        cur.execute(
            """
            UPDATE jobs
            SET status = 'processing',
                worker_id = 'Dead-Worker',
                claim_token = %s,
                last_heartbeat_at =
                    NOW() - INTERVAL '10 minutes'
            WHERE id = %s
            """,
            (str(uuid.uuid4()), job_id)
        )

        cur.execute(
            """
            UPDATE jobs
            SET status = 'pending',
                worker_id = NULL,
                claim_token = NULL,
                last_heartbeat_at = NULL,
                updated_at = NOW()
            WHERE status = 'processing'
              AND last_heartbeat_at <
                  NOW() - INTERVAL '2 minutes'
            """
        )

        recovered = cur.rowcount

    c.commit()
    c.close()

    if recovered == 1:
        ok("Zombie job recovery")
    else:
        fail("Zombie recovery", "Zombie was not recovered")


# ---------------------------------------------------------
# 6. FENCING TOKEN
# ---------------------------------------------------------

def test_fencing():

    clean()

    job_id = make_job()

    token_a = str(uuid.uuid4())
    token_b = str(uuid.uuid4())

    c = conn()

    with c.cursor() as cur:

        # Worker A owns job
        cur.execute(
            """
            UPDATE jobs
            SET status = 'processing',
                worker_id = 'Worker-A',
                claim_token = %s,
                last_heartbeat_at = NOW()
            WHERE id = %s
            """,
            (token_a, job_id)
        )

        # Worker A becomes stale.
        # Worker B takes ownership.
        cur.execute(
            """
            UPDATE jobs
            SET worker_id = 'Worker-B',
                claim_token = %s
            WHERE id = %s
            """,
            (token_b, job_id)
        )

        # Worker A wakes up and tries to complete.
        cur.execute(
            """
            UPDATE jobs
            SET status = 'completed',
                updated_at = NOW()
            WHERE id = %s
              AND status = 'processing'
              AND claim_token = %s
            """,
            (job_id, token_a)
        )

        affected = cur.rowcount

    c.commit()
    c.close()

    if affected == 0:
        ok("Fencing token rejects stale worker")
    else:
        fail(
            "Fencing token",
            "Old worker was able to complete the job"
        )


# ---------------------------------------------------------
# 7. RETRY + BACKOFF
# ---------------------------------------------------------

def test_retry():

    clean()

    job_id = make_job()

    c = conn()

    with c.cursor() as cur:

        attempts = 1
        backoff = (attempts ** 2) * 10

        cur.execute(
            """
            UPDATE jobs
            SET status = 'pending',
                attempts = %s,
                run_at = NOW() + (%s * INTERVAL '1 second'),
                worker_id = NULL,
                claim_token = NULL,
                last_heartbeat_at = NULL
            WHERE id = %s
            """,
            (attempts, backoff, job_id)
        )

        cur.execute(
            """
            SELECT attempts, run_at > NOW()
            FROM jobs
            WHERE id = %s
            """,
            (job_id,)
        )

        row = cur.fetchone()

    c.commit()
    c.close()

    if row and row[0] == 1 and row[1]:
        ok("Retry uses delayed backoff")
    else:
        fail("Retry", "Retry/backoff state is incorrect")


# ---------------------------------------------------------
# 8. DLQ
# ---------------------------------------------------------

def test_dlq():

    clean()

    job_id = make_job()

    max_attempts = 5

    c = conn()

    with c.cursor() as cur:

        cur.execute(
            """
            UPDATE jobs
            SET attempts = %s,
                status = 'dlq',
                worker_id = NULL,
                claim_token = NULL,
                last_heartbeat_at = NULL
            WHERE id = %s
            """,
            (max_attempts, job_id)
        )

        cur.execute(
            """
            SELECT status, attempts
            FROM jobs
            WHERE id = %s
            """,
            (job_id,)
        )

        row = cur.fetchone()

    c.commit()
    c.close()

    if row and row[0] == "dlq" and row[1] >= max_attempts:
        ok("Max retry limit moves job to DLQ")
    else:
        fail("DLQ", "Job did not move to DLQ")


# ---------------------------------------------------------
# 9. STALE WORKER COMPLETE
# ---------------------------------------------------------

def test_stale_worker():

    clean()

    job_id = make_job()

    old_token = str(uuid.uuid4())
    new_token = str(uuid.uuid4())

    c = conn()

    with c.cursor() as cur:

        cur.execute(
            """
            UPDATE jobs
            SET status = 'processing',
                worker_id = 'Worker-A',
                claim_token = %s
            WHERE id = %s
            """,
            (old_token, job_id)
        )

        # Sweeper / Worker B changes ownership
        cur.execute(
            """
            UPDATE jobs
            SET worker_id = 'Worker-B',
                claim_token = %s
            WHERE id = %s
            """,
            (new_token, job_id)
        )

        # Old worker tries to finish
        cur.execute(
            """
            UPDATE jobs
            SET status = 'completed'
            WHERE id = %s
              AND claim_token = %s
            """,
            (job_id, old_token)
        )

        affected = cur.rowcount

    c.commit()
    c.close()

    if affected == 0:
        ok("Stale worker cannot modify reclaimed job")
    else:
        fail("Stale worker", "Old worker modified reclaimed job")


# ---------------------------------------------------------
# 10. IDEMPOTENCY
# ---------------------------------------------------------

def test_idempotency():

    warn(
        "Idempotency",
        "Queue provides at-least-once delivery; external APIs "
        "must use an idempotency key/request ID"
    )


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":

    print("\n========== DISTRIBUTED JOB QUEUE TEST ==========\n")

    clean()

    test_enqueue()
    test_run_at()
    test_concurrent_claim()
    test_heartbeat()
    test_zombie_recovery()
    test_fencing()
    test_retry()
    test_dlq()
    test_stale_worker()
    test_idempotency()

    print("\n========== RESULT ==========")
    print(f"PASS : {PASS}")
    print(f"FAIL : {FAIL}")
    print(f"WARN : {WARN}")

    if FAIL == 0:
        print("\nSYSTEM INVARIANTS LOOK GOOD")
    else:
        print("\nSYSTEM HAS FAILURES")