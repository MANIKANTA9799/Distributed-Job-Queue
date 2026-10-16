import time


def recover_zombie_jobs(
    conn,
    timeout_seconds: int = 120
):

    sql = """
        UPDATE jobs
        SET status = 'pending',
            worker_id = NULL,
            claim_token = NULL,
            last_heartbeat_at = NULL,
            updated_at = NOW()
        WHERE status = 'processing'
          AND last_heartbeat_at < 
              NOW() - (%s * INTERVAL '1 second')
    """

    with conn.cursor() as cursor:

        cursor.execute(
            sql,
            (timeout_seconds,)
        )

        recovered_count = cursor.rowcount

        conn.commit()

        if recovered_count > 0:

            print(
                f"Sweeper recovered "
                f"{recovered_count} zombie jobs."
            )

        return recovered_count


def run_sweeper_loop(conn):

    print("Sweeper starting...")

    while True:

        recover_zombie_jobs(conn)

        time.sleep(60)