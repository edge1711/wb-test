# Data Engineering ETL Pipeline

## How to Run the Code Manually

### Step 1: Ensure Docker is Running
- **Mac/Windows**: Launch Docker Desktop application
- **Linux**: Start Docker daemon with `sudo systemctl start docker`

### Step 2: Build and Run the ETL Pipeline

```bash
# Build the Docker image
docker build -t etl .

# Run the container with volume mounts for persistent data
docker run --rm \
  -v $(pwd)/agg_result.db:/app/agg_result.db \
  -v $(pwd)/output:/app/output \
  etl
```

## CI/CD Pipeline

### Current Implementation
The CI/CD pipeline automatically triggers upon successful push to the `main` branch:

1. **Build Stage**: Creates a Docker image with all dependencies
2. **Execution Stage**: Runs the ETL container
3. **Artifact Storage**: Saves the following as GitHub Actions artifacts:
   - Updated database file (`agg_result.db`)
   - CSV file with top users analysis

In a production environment, the pipeline would be enhanced with a direct integration with cloud data warehouse (e.g., Snowflake, BigQuery),
securing credentials in the ENV variables or cloud KMS, data testing, etc.

## 📊 Scaling to 100+ Million Records

### Technology Stack Recommendations

#### Option 1: Cloud Storage + DuckLake (or Apache Iceberg) + Data Warehouse
- **Storage**: Apache Parquet files on S3/GCS for cost-effective raw data storage
- **Transformation**: Run DuckDB (with DuckLake extantion) on cloud VM for efficient columnar processing without memory constraints
- **Loading**: Save aggregated data into Pandas Dataframe for loading into data warehouse
- **Warehouse**: Cloud-native solutions (Snowflake, BigQuery, Redshift, ClickHouse)

#### Option 2: Big Data Stack
- **Storage**: HDFS for distributed file storage
- **Processing**: Apache Spark for distributed computing
- **Orchestration**: Apache Airflow for workflow management
- **Warehouse**: Cloud-native solutions (Snowflake, BigQuery, Redshift, ClickHouse)

#### Option 3: Polars
- **Framework**: Polars with lazy mode
- **Processing**: Lazy Frame with more efficient memory usage
- **Storage**: Partitioned Parquet files

### Proposed ETL/ELT Architecture

**Architecture Pattern**: ELT (Extract, Load, Transform) or EtLT
1. **Extract**: Ingest raw data with minimal processing
2. **Load**: Store in low-cost object storage (S3/GCS)
3. **Transform**: Apply business logic in the data warehouse using SQL

### Monitoring Metrics

#### Pipeline Health Metrics
- **Success Rate**: Percentage of successful pipeline runs
- **Execution Time**: End-to-end pipeline duration and stage-level timings
- **Data Volume**: Records processed per run
- **Error Rate**: Failed runs with categorized error types

#### Data Quality Metrics
- **Completeness**: Null value percentages per critical field
- **Uniqueness**: Duplicate record detection
- **Consistency**: Day-over-day variance analysis
- **Freshness**: Data latency from source to warehouse
- **Accuracy**: Business rule validation pass rates

### Data Storage Strategy

#### Input Data Storage
- **Hot Storage** (Frequent Access):
  - Recent data (< 30 days) in data warehouse
  - Optimized for query performance
  
- **Cold Storage** (Archival):
  - Historical data in object storage (S3/GCS)
  - Parquet format for compression and query efficiency

#### Output Data Storage
- **Primary Storage**: Data warehouse for analytics and reporting
  - Partitioned by date for query optimization
