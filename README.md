


Reproduce:

Terraform and infrastructure:
One time installation: 
1. Install terraform if not installed: https://developer.hashicorp.com/terraform/install)
2. Create a project in Google Cloud Platform (or you can use the existing one)
3. Create service account with roles Storage Admin + Storage Object Admin + BigQuery Admin
4. Enable these APIs for your project:
https://console.cloud.google.com/apis/library/iam.googleapis.com
https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
5. Create json key for a service account in GCP and put it under keys directory. For convenience I use one service accounts for all purposes, but with understanding that it should be different in production
6. Set enviromental variable: 
   - export GOOGLE_APPLICATION_CREDENTIALS="terraform/keys/your_key.json" (возможно тут изменения после переноса ключей, надо перепроверить)
7. Install SDK and initialize gcloud CLI: https://docs.cloud.google.com/sdk/docs/install-sdk (возможно нужно написать подробнее)
8. run 'gcloud auth application-default login'
9. update variables.tf if needed
7. run terraform init
8. check terraform plan if needed
9. run terraform apply 

Later:
- terraform apply 
- terraform destroy! when you don't need it anymore


Streaming Data
- for producer:
  - cd streaming-producer
  - uv sync 
  - uv run python producer.py 

All other:
- cd docker
- docker compose up --build -d (probably won't work somwehre since I have Apple M4. Need to be tested on another OC)
- docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/consumer2.py \
    --pyFiles /opt/src -d



TODO:
- describe that sometimes trigger_name in workflow should be changed
