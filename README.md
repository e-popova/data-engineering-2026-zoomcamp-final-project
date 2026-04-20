


# Steps to run the project locally:

## 1. Terraform and infrastructure:
1. For installation follow the instructions [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/terraform/README.md)
2. Update [terraform/variables.tf](terraform/variables.tf) 
3. Run:
```
cd terraform
terraform init
terraform apply 
cd ../
```
## 2. Starting the services
1. Setup environment variables at [docker/.env](docker/.env) 
2. To run services: redpanda, Kestra, Flink run:
```
cd docker
docker compose up --build -d 
cd ../
```
(It's possible that there can be problems with Flink image because it was tested on Apple M4)
3. Now you can check:

| Link                  | Description                                       |
| -----------           |---------------------------------------------------|
| http://localhost:8080 | redpanda console to see topics and messages       |
| http://localhost:8081 | Flink UI to see Jobs and tasks.                   |
| http://localhost:8082 | Kestra UI. Login/pass: admin@admin.com/Qwerty123! |

## 3. Starting data streaming
Source data will be coming from Binance as WebSocket messages and python script will forward it to Kafka. 
To run it:
1. Install uv if needed:
```
pip install uv 
```
2. Run python script:
```
cd streaming-producer
uv sync 
uv run python producer.py 
```
After that you will see logs about connecting to WebSocket and sending data to Kafka

## 4. Processing data streaming
Data from Kafka is consumed by Flink and then it sends data to GCS bucket as parquet files. 
To run Job:
1. Run in another terminal:
```
cd docker
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/consumer2.py \
    --pyFiles /opt/src -d
```
In Flink UI you should start to see a Job. 
Also the new directories and files should appear in GCS bucket but according to your checkpoint and partition intervals
## 5. Kestra and Data Warehouse
The data from GCS bucket should go to BigQuery. For this the following flows should be copied to Kestra (using Kestra UI):
- [create_raw_tables.yaml](kestra/create_raw_tables.yaml) - copy and run it manually. It will create raw tables
- [gcs_to_bigquery_trigger.yaml](kestra/gcs_to_bigquery_trigger.yaml) - copy and save. This flow will be triggered every time when partition is commited in GCS bucket and after that the data will go to BigQuery raw table
- [dbt_build.yaml](kestra/dbt_build.yaml) - copy and save. This flow will be triggered once per hour (can be changed). This will run `dbt build` which will build models so the data will appear in dbt dataset. 
  But before executing dbt_build workflow:
  - Change data in [profiles.yml](dbt/profiles.yml)
  - Restart the service:
```
cd docker
docker compose up -d kestra     
cd ../
```