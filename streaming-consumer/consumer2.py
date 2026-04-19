from pyflink.table import EnvironmentSettings, TableEnvironment
import os

GCS_BUCKET = os.getenv("GCS_BUCKET")
KEY_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
PARTITION_INTERVAL = int(os.getenv("PARTITION_INTERVAL", 60))
CHECKPOINT_INTERVAL = int(os.getenv("CHECKPOINT_INTERVAL", 30))

PARTITION_COMMIT_DELAY = PARTITION_INTERVAL + 10

# 1. Create environment
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(settings)
table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", f"{CHECKPOINT_INTERVAL} s")
table_env.get_config().get_configuration().set_string("table.exec.source.idle-timeout", "10 s")


# 2. Describe Kafka topic
table_env.execute_sql("""
    CREATE TABLE kafka_logs (
            `symbol` STRING,
            `open_time`   BIGINT,
            `close_time`  BIGINT,
            `open` DOUBLE,
            `high` DOUBLE,
            `low` DOUBLE,
            `close` DOUBLE,
            `volume` DOUBLE,
            `trades` INT,
            `ingested_at` STRING,
            `close_time_ts`  AS TO_TIMESTAMP_LTZ(`close_time`, 3),
             WATERMARK FOR `close_time_ts` AS `close_time_ts` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'klines',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'earliest',
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
        `symbol`      STRING,
        `open_time`   TIMESTAMP_LTZ(3),
        `close_time`  TIMESTAMP_LTZ(3),
        `open`        DOUBLE,
        `high`        DOUBLE,
        `low`         DOUBLE,
        `close`       DOUBLE,
        `volume`      DOUBLE,
        `trades`      INT,
        `ingested_at` TIMESTAMP_LTZ(3),
        `folder_ts`   STRING
    ) PARTITIONED BY (folder_ts) WITH (
        'connector' = 'filesystem',
        'path' = '{GCS_BUCKET}',
        'format' = 'parquet',
        'auto-compaction'                                = 'true',
        'compaction.file-size'                           = '128MB',
        'sink.partition-commit.trigger'                  = 'partition-time',
        'sink.partition-commit.delay'                    = '{PARTITION_COMMIT_DELAY} s',
        'partition.time-extractor.kind'                  = 'default',
        'partition.time-extractor.timestamp-pattern'     = '$folder_ts',
        'sink.partition-commit.policy.kind'              = 'success-file'
    )
""")

# 4. Read kafka messages and create files in GCS
table_env.execute_sql(f"""
    INSERT INTO gcs_sink
    SELECT `symbol`,
        TO_TIMESTAMP_LTZ(`open_time`, 3)  AS `open_time`,
        TO_TIMESTAMP_LTZ(`close_time`, 3) AS `close_time`,
        `open`, 
        `high`, 
        `low`, 
        `close`, 
        `volume`, 
        `trades`, 
        TO_TIMESTAMP_LTZ(
            UNIX_TIMESTAMP(REPLACE(SUBSTR(ingested_at, 1, 19), 'T', ' ')), 0
        ) AS `ingested_at`,
        DATE_FORMAT(
            TO_TIMESTAMP_LTZ(
                FLOOR(`close_time` / ({PARTITION_INTERVAL} * 1000)) * {PARTITION_INTERVAL},
                0
            ),
            'yyyy-MM-dd HH:mm:ss'
        ) AS `folder_ts`
    FROM kafka_logs
""").wait()



