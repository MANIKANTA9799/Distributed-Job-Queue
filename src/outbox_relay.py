import time
import redis


redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=0
)


def relay_outbox_jobs(conn, batch_size: int = 100) -> int:

    sql_select = """
        SELECT id, EXTRACT(EPOCH FROM run_at) AS score
        FROM jobs
        WHERE status = 'pending'
          AND is_pushed = FALSE
          AND run_at <= NOW()
        ORDER BY run_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT %s
    """

    with conn.cursor() as cursor:

        cursor.execute(
            sql_select,
            (batch_size,)
        )

        jobs = cursor.fetchall()

        if not jobs:
            conn.rollback()
            return 0

        pipeline = redis_client.pipeline()

        for job_id, score in jobs:

            pipeline.zadd(
                "jobs_queue",
                {
                    str(job_id): float(score)
                }
            )

        pipeline.execute()

        job_ids = [
            str(job_id)
            for job_id, _ in jobs
        ]

        sql_update = """
            UPDATE jobs
            SET is_pushed = TRUE,
                updated_at = NOW()
            WHERE id = ANY(%s::uuid[])
        """

        cursor.execute(
            sql_update,
            (job_ids,)
        )

    conn.commit()

    print(
        f"Outbox Relay pushed "
        f"{len(job_ids)} jobs to Redis."
    )

    return len(job_ids)


def run_outbox_relay_loop(conn):

    print("Outbox Relay starting...")

    while True:

        count = relay_outbox_jobs(conn)

        if count == 0:
            time.sleep(1)