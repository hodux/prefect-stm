from prefect import flow

SOURCE_REPO="."

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="stm-flow.py:push_stm_data", # Specific flow to run
    ).deploy(
        name="stm-deployment",
        work_pool_name="stm-work-pool", # Work pool target
        cron="*/5 * * * *", # Cron schedule (every 5 minutes)
    )

