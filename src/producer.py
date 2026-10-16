import uuid
import json
import redis

redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=0
)


def enqueue_job(conn, task_type: str, payload: dict) -> str:

    job_id = str(uuid.uuid4())

    sql = """
        INSERT INTO jobs (
            id,
            task_type,
            payload,
            status,
            is_pushed
        )
        VALUES (
            %s,
            %s,
            %s::jsonb,
            'pending',
            FALSE
        )
    """

    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                job_id,
                task_type,
                json.dumps(payload)
            )
        )

    conn.commit()

    # Push only after PostgreSQL has successfully committed.
    redis_client.zadd(
        "jobs_queue",
        {job_id: 0}
    )

    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE jobs
            SET is_pushed = TRUE,
                updated_at = NOW()
            WHERE id = %s
            """,
            (job_id,)
        )

    conn.commit()

    return job_id