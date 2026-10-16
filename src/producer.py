import uuid
import json


def enqueue_job(conn, task_type: str, payload: dict) -> str:
    job_id = str(uuid.uuid4())

    sql = """
        INSERT INTO jobs (
            id,
            task_type,
            payload
        )
        VALUES (%s, %s, %s::jsonb)
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

    return job_id