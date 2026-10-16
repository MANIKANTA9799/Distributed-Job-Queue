import uuid
import time


def claim_next_job(conn, worker_id: str):
    claim_token = str(uuid.uuid4())

    sql = """
        UPDATE jobs
        SET status = 'processing',
            worker_id = %s,
            claim_token = %s,
            last_heartbeat_at = NOW(),
            updated_at = NOW()
        WHERE id = (
            SELECT id
            FROM jobs
            WHERE status = 'pending'
              AND run_at <= NOW()
            ORDER BY run_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, task_type, payload, claim_token, attempts;
    """

    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (worker_id, claim_token)
        )

        job = cursor.fetchone()

        conn.commit()

        return job


def heartbeat_job(conn, job_id: str, claim_token: str) -> bool:

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
            (job_id, claim_token)
        )

        success = cursor.rowcount == 1

        conn.commit()

        return success


def complete_job(conn, job_id: str, claim_token: str) -> bool:

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
            (job_id, claim_token)
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

    if new_attempts >= max_attempts:

        sql = """
            UPDATE jobs
            SET status = 'dlq',
                attempts = %s,
                worker_id = NULL,
                claim_token = NULL,
                last_heartbeat_at = NULL,
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

    else:

        backoff_seconds = (new_attempts ** 2) * 10

        sql = """
            UPDATE jobs
            SET status = 'pending',
                attempts = %s,
                worker_id = NULL,
                claim_token = NULL,
                last_heartbeat_at = NULL,
                run_at = NOW() + (%s * INTERVAL '1 second'),
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


def process_task(task_type, payload):

    print(
        f"Processing {task_type}: {payload}"
    )

    # Simulate actual work
    time.sleep(2)


def run_worker_loop(conn, worker_id: str):

    print(f"Worker {worker_id} starting...")

    while True:

        job = claim_next_job(
            conn,
            worker_id
        )

        if not job:
            time.sleep(1)
            continue

        (
            job_id,
            task_type,
            payload,
            claim_token,
            attempts
        ) = job

        print(
            f"Claimed job {job_id} "
            f"({task_type}) "
            f"attempt={attempts + 1}"
        )

        try:

            process_task(
                task_type,
                payload
            )

            success = complete_job(
                conn,
                job_id,
                claim_token
            )

            if success:

                print(
                    f"Job {job_id} completed successfully."
                )

            else:

                print(
                    f"Worker {worker_id} "
                    f"lost ownership of {job_id}."
                )

        except Exception as e:

            print(
                f"Job {job_id} failed: {e}"
            )

            fail_job(
                conn,
                job_id,
                claim_token,
                attempts
            )