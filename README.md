# Overview
A hobby project to practice my DE skills. The focus is building a medallion style ELT data pipeline to process NOAA station and gridded QPE rainfall. This is a work in progress :) 

Stack
- Python
- Flask
- SQL
- Docker
- GCP (BigQuery, Cloud Storage, Cloud Run Service)

Airflow Initialization:

https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

- Fetch latest docker compose: 
```
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.2.1/docker-compose.yaml'
```
- Configure airflow user:
```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
- Initialize database
```
docker compose up airflow-init
```
- To stop and delete containers, delete volumes with database data and download images, run:
```
docker compose down --volumes --rmi all
```

- Run airflow `docker compose up`
- Visit http://localhost:8080

