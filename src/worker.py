import os
import uuid
import time
import threading
import redis
import psycopg2


DB_DSN = os.environ["DB_DSN"]

redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True
)


def claim_job_by_id(
    conn,
    job_id: str,
    worker_id: str
):
    claim_token = str(uuid.uuid4())

    sql = """
        UPDATE jobs
        SET status = 'processing',
            worker_id = %s,
            claim_token = %s,
            last_heartbeat_at = NOW(),
            updated_at = NOW()
        WHERE id = %s
          AND status = 'pending'
          AND run_at <= NOW()
        RETURNING
            id,
            task_type,
            payload,
            claim_token,
            attempts
    """

    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                worker_id,
                claim_token,
                job_id
            )
        )

        job = cursor.fetchone()

    conn.commit()

    return job


def heartbeat_job(
    conn,
    job_id: str,
    claim_token: str
) -> bool:

    sql = """
        UPDATE jobs
        SET last_heartbeat_at = NOW(),
            updated_at = NOW()
        WHERE id = %s
          AND status = 'processing'
          AND claim_token = %s
    """

    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                job_id,
                claim_token
            )
        )

        success = cursor.rowcount == 1

    conn.commit()

    return success


def heartbeat_loop(
    job_id: str,
    claim_token: str,
    stop_event: threading.Event
):
    conn = psycopg2.connect(DB_DSN)

    try:
        while not stop_event.wait(5):
            success = heartbeat_job(
                conn,
                job_id,
                claim_token
            )

            if not success:
                print(
                    f"Heartbeat failed. "
                    f"Lost ownership of {job_id}."
                )
                break

    finally:
        conn.close()


def complete_job(
    conn,
    job_id: str,
    claim_token: str
) -> bool:

    sql = """
        UPDATE jobs
        SET status = 'completed',
            worker_id = NULL,
            claim_token = NULL,
            last_heartbeat_at = NULL,
            updated_at = NOW()
        WHERE id = %s
          AND status = 'processing'
          AND claim_token = %s
    """

    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                job_id,
                claim_token
            )
        )

        success = cursor.rowcount == 1

    conn.commit()

    return success


def fail_job(
    conn,
    job_id: str,
    claim_token: str,
    current_attempts: int,
    max_attempts: int = 5
):
    new_attempts = current_attempts + 1

    # Move permanently failed jobs to DLQ.
    if new_attempts >= max_attempts:

        sql = """
            UPDATE jobs
            SET status = 'dlq',
                attempts = %s,
                worker_id = NULL,
                claim_token = NULL,
                last_heartbeat_at = NULL,
                is_pushed = FALSE,
                updated_at = NOW()
            WHERE id = %s
              AND claim_token = %s
              AND status = 'processing'
        """

        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    new_attempts,
                    job_id,
                    claim_token
                )
            )

        conn.commit()

        print(
            f"Job {job_id} moved to DLQ "
            f"after {new_attempts} attempts."
        )

        return

    # Retry with delayed backoff.
    backoff_seconds = (new_attempts ** 2) * 10

    sql = """
        UPDATE jobs
        SET status = 'pending',
            attempts = %s,
            worker_id = NULL,
            claim_token = NULL,
            last_heartbeat_at = NULL,
            is_pushed = FALSE,
            run_at = NOW() +
                (%s * INTERVAL '1 second'),
            updated_at = NOW()
        WHERE id = %s
          AND claim_token = %s
          AND status = 'processing'
    """

    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                new_attempts,
                backoff_seconds,
                job_id,
                claim_token
            )
        )

    conn.commit()

    print(
        f"Job {job_id} will retry in "
        f"{backoff_seconds} seconds."
    )


def process_task(
    task_type,
    payload
):
    print(
        f"Processing {task_type}: "
        f"{payload}"
    )

    # Simulate actual work.
    time.sleep(10)


def get_due_job():
    """
    Get the earliest Redis job only if its
    run_at score has been reached.
    """

    items = redis_client.zrange(
        "jobs_queue",
        0,
        0,
        withscores=True
    )

    if not items:
        return None

    job_id, score = items[0]#type: ignore 

    if score > time.time():
        return None

    removed = redis_client.zpopmin(
        "jobs_queue",
        1
    )

    if not removed:
        return None

    return removed[0][0] #type: ignore 


def run_redis_worker_loop(
    conn,
    worker_id: str
):
    print(
        f"Redis Worker {worker_id} starting..."
    )

    while True:

        job_id = get_due_job()

        if not job_id:
            time.sleep(0.1)
            continue

        job = claim_job_by_id(
            conn,
            job_id,
            worker_id
        )

        # Another worker may have already claimed it.
        if not job:
            continue

        (
            job_id,
            task_type,
            payload,
            claim_token,
            attempts
        ) = job

        print(
            f"[{worker_id}] "
            f"Claimed {job_id} "
            f"attempt={attempts + 1}"
        )

        stop_event = threading.Event()

        heartbeat_thread = threading.Thread(
            target=heartbeat_loop,
            args=(
                str(job_id),
                claim_token,
                stop_event
            ),
            daemon=True
        )

        heartbeat_thread.start()

        try:
            process_task(
                task_type,
                payload
            )

            stop_event.set()

            success = complete_job(
                conn,
                str(job_id),
                claim_token
            )

            if success:
                print(
                    f"[{worker_id}] "
                    f"Job {job_id} completed."
                )
            else:
                print(
                    f"[{worker_id}] "
                    f"Lost ownership of {job_id}."
                )

        except Exception as e:

            stop_event.set()

            print(
                f"[{worker_id}] "
                f"Job {job_id} failed: {e}"
            )

            fail_job(
                conn,
                str(job_id),
                claim_token,
                attempts
            )