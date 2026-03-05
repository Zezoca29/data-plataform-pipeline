# Data Platform Pipeline (End-to-End)

Plataforma de engenharia de dados **100% local e gratuita** para portfólio, simulando um ambiente real de empresas de tecnologia de grande escala.

## Objetivo
Construir uma plataforma moderna com:
- ingestão de dados públicos (CoinGecko)
- streaming em tempo quase real
- processamento distribuído
- data lake em formato Parquet
- transformações analíticas com dbt
- data warehouse para consumo analítico
- dashboards em BI
- orquestração ponta a ponta com Airflow

## Arquitetura
Fluxo:

`CoinGecko API -> Python Ingestion -> Kafka -> Spark -> MinIO (Parquet) -> dbt -> PostgreSQL -> Superset`

Consulte o diagrama completo em [`architecture/architecture.md`](architecture/architecture.md).

## Stack tecnológica
- Python
- Apache Airflow
- Apache Kafka + Zookeeper
- Apache Spark (PySpark)
- MinIO (S3-compatible Data Lake)
- PostgreSQL (Data Warehouse)
- dbt (dbt-postgres)
- Apache Superset
- Docker + Docker Compose
- Parquet

## Estrutura do repositório
```text
data-plataform-pipeline
├── docker-compose.yml
├── README.md
├── ingestion/
│   ├── api_collector.py
│   └── Dockerfile
├── streaming/
│   ├── kafka_producer.py
│   └── kafka_consumer.py
├── spark_jobs/
│   └── process_data.py
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── data_pipeline.py
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       └── marts/
├── warehouse/
│   ├── init.sql
│   └── example_queries.sql
├── dashboards/
│   └── superset_dataset_notes.md
├── scripts/
│   ├── postgres_connection.py
│   └── analytical_query.py
└── architecture/
    └── architecture.md
```

## Como rodar
### 1) Subir a infraestrutura
```bash
docker compose up -d --build
```

### 2) Criar tópico no Kafka (uma única vez)
```bash
docker compose exec kafka kafka-topics --create --topic crypto_prices_raw --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```

### 3) Inicializar Airflow (DB + usuário admin)
```bash
docker compose exec airflow-webserver airflow db migrate
docker compose exec airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@local --password admin
```

### 4) Executar pipeline
- Acesse Airflow: http://localhost:8088
- Habilite DAG `crypto_data_platform`
- Rode manualmente a DAG para bootstrap

### 5) Acessar serviços
- MinIO Console: http://localhost:9001 (`minio` / `minio123`)
- Superset: http://localhost:8089 (`admin` / `admin`)
- PostgreSQL: `localhost:5432`

## Exemplos de código entregues
- Ingestão Python: `ingestion/api_collector.py`
- Kafka Producer/Consumer: `streaming/kafka_producer.py` e `streaming/kafka_consumer.py`
- PySpark streaming: `spark_jobs/process_data.py`
- Airflow DAG: `airflow/dags/data_pipeline.py`
- dbt models: `dbt_project/models/staging` e `dbt_project/models/marts`
- Conexão PostgreSQL: `scripts/postgres_connection.py`
- Consulta analítica: `scripts/analytical_query.py` e `warehouse/example_queries.sql`

## Exemplo de query analítica
```sql
select
    symbol,
    date_trunc('hour', collected_ts) as collected_hour,
    avg(price) as avg_price,
    avg(change_24h) as avg_change_24h
from public.stg_crypto_prices
group by 1,2
order by collected_hour desc;
```

## Fluxo dos dados no pipeline
1. `api_collector.py` coleta preços de cripto da API pública.
2. Eventos são publicados no Kafka (`crypto_prices_raw`).
3. `process_data.py` consome o tópico e aplica parse/schema em streaming.
4. Dados são gravados em Parquet no MinIO (`s3a://lakehouse/bronze/crypto_prices`).
5. Em paralelo, os dados raw são persistidos em `fact_crypto_prices_raw` no PostgreSQL.
6. dbt transforma raw -> staging -> marts analíticas.
7. Superset lê as tabelas mart para dashboards executivos.

## Comandos úteis
```bash
# Logs de um serviço
docker compose logs -f airflow-webserver

# Rodar dbt manualmente
docker compose run --rm dbt run --profiles-dir .

# Testar consumer local
docker compose run --rm ingestion python /app/streaming/kafka_consumer.py

# Derrubar ambiente
docker compose down -v
```

## Melhorias futuras
- Data quality automática (Great Expectations/Soda).
- Testes de contratos e schema evolution no Kafka.
- Observabilidade fim a fim (OpenLineage + Marquez + Prometheus/Grafana).
- CI/CD com lint, testes e smoke test de DAG em GitHub Actions.
- Camadas Silver/Gold com regras de negócio e SCD.
- IaC com Terraform para ambiente cloud (AWS/GCP/Azure).

---
Projeto ideal para portfólio de Engenharia de Dados com foco em arquitetura moderna, streaming e analytics operacional.
