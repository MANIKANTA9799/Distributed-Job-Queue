import psycopg2
import threading
import time

from src.producer import enqueue_job
from src.worker import run_worker_loop
from src.sweeper import run_sweeper_loop


DB_DSN = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"


def setup_db():
    conn = psycopg2.connect(DB_DSN)

    with conn.cursor() as cursor:
        with open("db/01_init_schema.sql", "r") as file:
            sql = file.read()

        cursor.execute(sql)

    conn.commit()
    conn.close()

    print("Database setup complete.")


if __name__ == "__main__":

    # 1. Create the jobs table and indexes
    setup_db()

    # 2. Create separate connections
    conn_producer = psycopg2.connect(DB_DSN)
    conn_worker = psycopg2.connect(DB_DSN)
    conn_sweeper = psycopg2.connect(DB_DSN)

    # 3. Start worker
    worker_thread = threading.Thread(
        target=run_worker_loop,
        args=(conn_worker, "Worker-1"),
        daemon=True
    )

    worker_thread.start()

    # 4. Start sweeper
    sweeper_thread = threading.Thread(
        target=run_sweeper_loop,
        args=(conn_sweeper,),
        daemon=True
    )

    sweeper_thread.start()

    # 5. Produce jobs
    print("Producing jobs...")

    job1 = enqueue_job(
        conn_producer,
        "SEND_EMAIL",
        {
            "user_id": 1,
            "email": "alice@test.com"
        }
    )

    print(f"Created job: {job1}")

    job2 = enqueue_job(
        conn_producer,
        "PROCESS_PAYMENT",
        {
            "user_id": 2,
            "amount": 50
        }
    )

    print(f"Created job: {job2}")

    # 6. Let worker process jobs
    time.sleep(30)

    # 7. Close connections
    conn_producer.close()
    conn_worker.close()
    conn_sweeper.close()

    print("Test complete.")