import os
import time
import uuid
import threading

import redis
import psycopg2

from src.producer import enqueue_job
from src.sweeper import (
    relay_outbox_jobs,
    recover_zombie_jobs
)
from src.worker import (
    claim_job_by_id,
    heartbeat_job,
    complete_job,
    fail_job
)


DB_DSN = os.environ["DB_DSN"]

redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True
)


def get_conn():
    return psycopg2.connect(DB_DSN)


def cleanup_job(job_id):
    conn = get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM jobs
                WHERE id = %s
                """,
                (job_id,)
            )

        conn.commit()

    finally:
        conn.close()


def test_redis_connection():
    print("\n[TEST] Redis connection")

    assert redis_client.ping() is True

    print("[PASS] Redis connection")


def test_schema():
    print("\n[TEST] Schema")

    conn = get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'jobs'
                  AND column_name = 'is_pushed'
                """
            )

            row = cursor.fetchone()

        assert row is not None

    finally:
        conn.close()

    print("[PASS] is_pushed column exists")


def test_producer():
    print("\n[TEST] Producer -> PostgreSQL -> Redis")

    job_id = None
    conn = get_conn()

    try:
        job_id = enqueue_job(
            conn,
            "TEST_EMAIL",
            {
                "email": "test@example.com"
            }
        )

        score = redis_client.zscore(
            "jobs_queue",
            job_id
        )

        assert score is not None

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, is_pushed
                FROM jobs
                WHERE id = %s
                """,
                (job_id,)
            )

            row = cursor.fetchone()

        assert row is not None
        assert row[0] == "pending"
        assert row[1] is True

        print(
            "[PASS] Producer creates job "
            "and pushes to Redis"
        )

    finally:
        if job_id:
            redis_client.zrem(
                "jobs_queue",
                job_id
            )

            cleanup_job(job_id)

        conn.close()


def test_outbox_relay():
    print("\n[TEST] Outbox Relay")

    job_id = str(uuid.uuid4())
    conn = get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO jobs (
                    id,
                    task_type,
                    payload,
                    status,
                    is_pushed
                )
                VALUES (
                    %s,
                    'OUTBOX_TEST',
                    '{"test": true}',
                    'pending',
                    FALSE
                )
                """,
                (job_id,)
            )

        conn.commit()

        redis_client.zrem(
            "jobs_queue",
            job_id
        )

        count = relay_outbox_jobs(
            conn,
            batch_size=100
        )

        assert count >= 1

        score = redis_client.zscore(
            "jobs_queue",
            job_id
        )

        assert score is not None

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT is_pushed
                FROM jobs
                WHERE id = %s
                """,
                (job_id,)
            )

            row = cursor.fetchone()

        assert row is not None
        assert row[0] is True

        print(
            "[PASS] Outbox Relay repairs "
            "stranded job"
        )

    finally:
        redis_client.zrem(
            "jobs_queue",
            job_id
        )

        cleanup_job(job_id)
        conn.close()


def test_claim():
    print("\n[TEST] PostgreSQL CAS claim")

    job_id = str(uuid.uuid4())
    conn = get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO jobs (
                    id,
                    task_type,
                    payload,
                    status,
                    is_pushed
                )
                VALUES (
                    %s,
                    'CLAIM_TEST',
                    '{}',
                    'pending',
                    TRUE
                )
                """,
                (job_id,)
            )

        conn.commit()

        redis_client.zadd(
            "jobs_queue",
            {
                job_id: time.time()
            }
        )

        job = claim_job_by_id(
            conn,
            job_id,
            "Worker-Test"
        )

        assert job is not None

        # PostgreSQL UUID may be returned as a UUID
        # object or as a string depending on configuration.
        assert str(job[0]) == job_id

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, worker_id
                FROM jobs
                WHERE id = %s
                """,
                (job_id,)
            )

            row = cursor.fetchone()

        assert row is not None
        assert row[0] == "processing"
        assert row[1] == "Worker-Test"

        print("[PASS] PostgreSQL CAS claim")

    finally:
        redis_client.zrem(
            "jobs_queue",
            job_id
        )

        cleanup_job(job_id)
        conn.close()


def test_concurrent_claim():
    print("\n[TEST] Concurrent workers")

    job_id = str(uuid.uuid4())

    conn = get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO jobs (
                    id,
                    task_type,
                    payload,
                    status,
                    is_pushed
                )
                VALUES (
                    %s,
                    'CONCURRENT_TEST',
                    '{}',
                    'pending',
                    TRUE
                )
                """,
                (job_id,)
            )

        conn.commit()

    finally:
        conn.close()

    results = []

    def worker(name):
        conn = get_conn()

        try:
            result = claim_job_by_id(
                conn,
                job_id,
                name
            )

            results.append(
                result is not None
            )

        finally:
            conn.close()

    thread_1 = threading.Thread(
        target=worker,
        args=("Worker-A",)
    )

    thread_2 = threading.Thread(
        target=worker,
        args=("Worker-B",)
    )

    thread_1.start()
    thread_2.start()

    thread_1.join()
    thread_2.join()

    assert sum(results) == 1

    print(
        "[PASS] Concurrent workers cannot "
        "claim same job"
    )

    cleanup_job(job_id)


def test_heartbeat():
    print("\n[TEST] Heartbeat")

    job_id = str(uuid.uuid4())
    claim_token = str(uuid.uuid4())

    conn = get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO jobs (
                    id,
                    task_type,
                    payload,
                    status,
                    worker_id,
                    claim_token,
                    last_heartbeat_at,
                    is_pushed
                )
                VALUES (
                    %s,
                    'HEARTBEAT_TEST',
                    '{}',
                    'processing',
                    'Worker-HB',
                    %s,
                    NOW() - INTERVAL '30 seconds',
                    TRUE
                )
                """,
                (
                    job_id,
                    claim_token
                )
            )

        conn.commit()

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT last_heartbeat_at
                FROM jobs
                WHERE id = %s
                """,
                (job_id,)
            )

            before = cursor.fetchone()

        assert before is not None

        success = heartbeat_job(
            conn,
            job_id,
            claim_token
        )

        assert success is True

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT last_heartbeat_at
                FROM jobs
                WHERE id = %s
                """,
                (job_id,)
            )

            after = cursor.fetchone()

        assert after is not None
        assert after[0] > before[0]

        print(
            "[PASS] Heartbeat updates timestamp"
        )

    finally:
        cleanup_job(job_id)
        conn.close()


def test_fencing():
    print("\n[TEST] Fencing token")

    job_id = str(uuid.uuid4())
    old_token = str(uuid.uuid4())
    current_token = str(uuid.uuid4())

    conn = get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO jobs (
                    id,
                    task_type,
                    payload,
                    status,
                    worker_id,
                    claim_token,
                    is_pushed
                )
                VALUES (
                    %s,
                    'FENCING_TEST',
                    '{}',
                    'processing',
                    'Worker-B',
                    %s,
                    TRUE
                )
                """,
                (
                    job_id,
                    current_token
                )
            )

        conn.commit()

        success = complete_job(
            conn,
            job_id,
            old_token
        )

        assert success is False

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT status
                FROM jobs
                WHERE id = %s
                """,
                (job_id,)
            )

            row = cursor.fetchone()

        assert row is not None
        assert row[0] == "processing"

        print(
            "[PASS] Stale worker rejected "
            "by fencing token"
        )

    finally:
        cleanup_job(job_id)
        conn.close()


def test_retry():
    print("\n[TEST] Retry")

    job_id = str(uuid.uuid4())
    claim_token = str(uuid.uuid4())

    conn = get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO jobs (
                    id,
                    task_type,
                    payload,
                    status,
                    claim_token,
                    attempts,
                    is_pushed
                )
                VALUES (
                    %s,
                    'RETRY_TEST',
                    '{}',
                    'processing',
                    %s,
                    1,
                    TRUE
                )
                """,
                (
                    job_id,
                    claim_token
                )
            )

        conn.commit()

        fail_job(
            conn,
            job_id,
            claim_token,
            1,
            max_attempts=5
        )

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    status,
                    attempts,
                    is_pushed,
                    run_at
                FROM jobs
                WHERE id = %s
                """,
                (job_id,)
            )

            row = cursor.fetchone()

        assert row is not None
        assert row[0] == "pending"
        assert row[1] == 2
        assert row[2] is False
        assert row[3] is not None

        print(
            "[PASS] Retry resets is_pushed "
            "and delays job"
        )

    finally:
        cleanup_job(job_id)
        conn.close()


def test_dlq():
    print("\n[TEST] DLQ")

    job_id = str(uuid.uuid4())
    claim_token = str(uuid.uuid4())

    conn = get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO jobs (
                    id,
                    task_type,
                    payload,
                    status,
                    claim_token,
                    attempts,
                    is_pushed
                )
                VALUES (
                    %s,
                    'DLQ_TEST',
                    '{}',
                    'processing',
                    %s,
                    4,
                    TRUE
                )
                """,
                (
                    job_id,
                    claim_token
                )
            )

        conn.commit()

        fail_job(
            conn,
            job_id,
            claim_token,
            4,
            max_attempts=5
        )

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, attempts
                FROM jobs
                WHERE id = %s
                """,
                (job_id,)
            )

            row = cursor.fetchone()

        assert row is not None
        assert row[0] == "dlq"
        assert row[1] == 5

        print(
            "[PASS] Max retries move job to DLQ"
        )

    finally:
        cleanup_job(job_id)
        conn.close()


def test_zombie_recovery():
    print("\n[TEST] Zombie recovery")

    job_id = str(uuid.uuid4())
    claim_token = str(uuid.uuid4())

    conn = get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO jobs (
                    id,
                    task_type,
                    payload,
                    status,
                    worker_id,
                    claim_token,
                    last_heartbeat_at,
                    is_pushed
                )
                VALUES (
                    %s,
                    'ZOMBIE_TEST',
                    '{}',
                    'processing',
                    'Dead-Worker',
                    %s,
                    NOW() - INTERVAL '10 minutes',
                    TRUE
                )
                """,
                (
                    job_id,
                    claim_token
                )
            )

        conn.commit()

        redis_client.zrem(
            "jobs_queue",
            job_id
        )

        count = recover_zombie_jobs(
            conn,
            timeout_seconds=120
        )

        assert count >= 1

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    status,
                    worker_id,
                    claim_token,
                    is_pushed
                FROM jobs
                WHERE id = %s
                """,
                (job_id,)
            )

            row = cursor.fetchone()

        assert row is not None
        assert row[0] == "pending"
        assert row[1] is None
        assert row[2] is None
        assert row[3] is False

        print("[PASS] Zombie job recovered")

    finally:
        cleanup_job(job_id)
        conn.close()


def test_redis_duplicate():
    print("\n[TEST] Redis duplicate protection")

    job_id = str(uuid.uuid4())

    try:
        redis_client.zadd(
            "jobs_queue",
            {
                job_id: time.time()
            }
        )

        redis_client.zadd(
            "jobs_queue",
            {
                job_id: time.time()
            }
        )

        members = redis_client.zrange(
            "jobs_queue",
            0,
            -1
        )

        # The same ZSET member can exist only once.
        assert members.count(job_id) == 1 #type: ignore

        print(
            "[PASS] Redis ZSET prevents "
            "duplicate job IDs"
        )

    finally:
        redis_client.zrem(
            "jobs_queue",
            job_id
        )


def run_all_tests():
    print()
    print("=" * 60)
    print("       DISTRIBUTED JOB QUEUE - REDIS TEST")
    print("=" * 60)

    tests = [
        test_redis_connection,
        test_schema,
        test_producer,
        test_outbox_relay,
        test_claim,
        test_concurrent_claim,
        test_heartbeat,
        test_fencing,
        test_retry,
        test_dlq,
        test_zombie_recovery,
        test_redis_duplicate
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1

        except Exception as e:
            failed += 1
            print(
                f"[FAIL] {test.__name__}: "
                f"{type(e).__name__}: {e}"
            )

    print()
    print("=" * 60)
    print("RESULT")
    print("=" * 60)
    print(f"PASS : {passed}")
    print(f"FAIL : {failed}")
    print("=" * 60)

    if failed == 0:
        print("ALL TESTS PASSED")
    else:
        print("SOME TESTS FAILED")


if __name__ == "__main__":
    run_all_tests()