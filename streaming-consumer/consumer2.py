from pyflink.table import EnvironmentSettings, TableEnvironment
import os

GCS_BUCKET = os.getenv("GCS_BUCKET")
KEY_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
PARTITION_INTERVAL = os.getenv("PARTITION_INTERVAL", 120)
CHECKPOINT_INTERVAL = os.getenv("CHECKPOINT_INTERVAL", 60)

# 1. Create environment
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(settings)
table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", f"{CHECKPOINT_INTERVAL} s")

# 2. Describe Kafka topic
table_env.execute_sql("""
    CREATE TABLE kafka_logs (
            `symbol` STRING,
            `open_time` STRING,
            `close_time` STRING,
            `open` DOUBLE,
            `high` DOUBLE,
            `low` DOUBLE,
            `close` DOUBLE,
            `volume` DOUBLE,
            `trades` INT,
            `ingested_at` STRING,
            `proc_time` AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'klines',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
""")

# 3. Describe filesystem connector
conf = table_env.get_config().get_configuration()
conf.set_string("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set_string("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
conf.set_string("fs.gs.project.id", GOOGLE_PROJECT_ID)
conf.set_string("fs.gs.auth.service.account.json.keyfile", KEY_FILE)

table_env.execute_sql(f"""
    CREATE TABLE gcs_sink (
        `symbol` STRING,
        `open_time` STRING,
        `close_time` STRING,
        `open` DOUBLE,
        `high` DOUBLE,
        `low` DOUBLE,
        `close` DOUBLE,
        `volume` DOUBLE,
        `trades` INT,
        `ingested_at` STRING,
        `proc_time` TIMESTAMP_LTZ(3),
        `proc_time2` AS PROCTIME(),
        `folder_ts` STRING
    ) PARTITIONED BY (folder_ts) WITH (
        'connector' = 'filesystem',
        'path' = '{GCS_BUCKET}',
        'format' = 'parquet',
        'sink.rolling-policy.check-interval'      = '10 s',
        'auto-compaction'                         = 'true',
        'sink.partition-commit.trigger'           = 'process-time',
        'sink.partition-commit.delay'             = '10 s',
        'sink.partition-commit.policy.kind'       = 'success-file'
    )
""")

# 4. Read kafka messages and create files in GCS
table_env.execute_sql(f"""
    INSERT INTO gcs_sink
    SELECT `symbol`, 
        `open_time`, 
        `close_time`, 
        `open`, `high`, 
        `low`, 
        `close`, 
        `volume`, 
        `trades`, 
        `ingested_at`,
        `proc_time`, 
        DATE_FORMAT(
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(CAST(proc_time AS STRING)) / {PARTITION_INTERVAL}) * {PARTITION_INTERVAL}), 
            'yyyy-MM-dd-HH-mm'  
        ) AS folder_ts
    FROM kafka_logs
""").wait()