# BrainRocket Gaming Data Pipeline

A comprehensive data engineering demo project showcasing a gaming-style data pipeline with batch ETL, streaming processing, and orchestration.

## 🚀 60-Second Quickstart

```bash
# 1. Start database and generate sample data
docker compose up -d
python etl/data_generator.py --num-records 1000 --output data/sample_transactions.csv

# 2. Run ETL pipeline to load data
python etl/etl_batch.py --input data/sample_transactions.csv

# 3. Query results in Adminer (http://localhost:8080)
#    Database: gaming_db, User: postgres, Password: postgres
```

*Airflow install is optional—code demonstrates DAG structure; run batch ETL without Airflow in two commands.*

## 🎮 Overview

This project demonstrates a complete data pipeline for gaming transaction data, featuring:

- **Batch ETL Processing**: CSV ingestion, data validation, and loading to PostgreSQL
- **Streaming Simulation**: Simulated Kafka producer/consumer for real-time data
- **Orchestration**: Airflow DAG for scheduled pipeline execution
- **Data Modeling**: Star schema with dimensions, facts, and materialized views
- **Monitoring**: Comprehensive logging and error handling

## 📁 Project Structure

```
brainrocket-data-pipeline/
├── etl/                    # ETL scripts
│   ├── utils.py           # Database utilities and helpers
│   ├── data_generator.py  # Synthetic data generation
│   └── etl_batch.py       # Main ETL pipeline
├── sql/                   # Database schema and seed data
│   ├── schema.sql         # Table definitions and indexes
│   └── seed_data.sql      # Dimension table seed data
├── orchestration/         # Airflow DAG
│   └── gaming_pipeline_dag.py
├── streaming/             # Simulated Kafka streaming
│   ├── producer.py        # Event producer
│   └── consumer.py        # Event consumer
├── data/                  # Generated data files
├── docker-compose.yml     # Postgres + Adminer setup
├── requirements.txt       # Python dependencies
├── .env.example          # Environment variables template
└── README.md             # This file
```

## 🚀 Quick Start

### 1. Prerequisites

- Docker and Docker Compose
- Python 3.10+
- pip (Python package manager)

### 2. Setup Database

```bash
# Copy environment variables template
cp .env.example .env

# Start PostgreSQL and Adminer
docker compose up -d

# Verify containers are running
docker ps

# Access Adminer UI at http://localhost:8080
# Login with: System=PostgreSQL, Server=postgres, Username=postgres, Password=postgres, Database=gaming_db
```

### 3. Install Python Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 4. Initialize Database

```bash
# Run the ETL pipeline with database initialization
python etl/etl_batch.py --init-db --input data/sample_transactions.csv

# Or generate sample data first
python etl/data_generator.py --num-records 1000 --output data/sample_transactions.csv
python etl/etl_batch.py --input data/sample_transactions.csv
```

### 5. Run Streaming Demo

```bash
# Terminal 1: Start consumer
python streaming/consumer.py --batch-size 50

# Terminal 2: Produce events
python streaming/producer.py --rate 2 --max-events 100 | python streaming/consumer.py --batch-size 50

# Or use pipes for continuous streaming
python streaming/producer.py --rate 0.5 | python streaming/consumer.py --batch-size 20
```

## 📊 Database Schema

### Dimension Tables
- `dim_game`: Game information (name, genre, price, etc.)
- `dim_location`: Geographic locations (countries, regions)

### Fact Table
- `fact_transactions`: Transaction events with foreign keys to dimensions

### Views
- `mv_daily_game_metrics`: Materialized view for daily aggregations
- `v_daily_game_report`: Reporting view with business metrics

## 🔧 Usage Examples

### Generate Sample Data
```bash
python etl/data_generator.py --num-records 500 --output data/new_transactions.csv
```

### Run ETL Pipeline
```bash
# Basic usage
python etl/etl_batch.py --input data/transactions.csv

# With table truncation
python etl/etl_pipeline.py --input data/transactions.csv --truncate

# Initialize database and load data
python etl/etl_batch.py --init-db --input data/transactions.csv
```

### Query Reports
```sql
-- Daily revenue report
SELECT * FROM v_daily_game_report ORDER BY transaction_day DESC, total_revenue DESC;

-- Game performance summary
SELECT 
    game_name,
    game_genre,
    COUNT(*) as total_transactions,
    SUM(total_revenue) as total_revenue,
    AVG(avg_transaction_value) as avg_txn_value
FROM v_daily_game_report 
GROUP BY game_name, game_genre 
ORDER BY total_revenue DESC;
```

## 🎯 Features

### Data Quality
- Schema validation and data cleansing
- Type conversion and normalization
- Duplicate and outlier detection
- Comprehensive error logging

### Performance
- Batch processing for efficiency
- Database indexing for fast queries
- Materialized views for reporting
- Connection pooling and reuse

### Monitoring
- Structured logging to file and console
- Progress tracking and metrics
- Error handling and retry logic
- Performance timing and statistics

## 🛠️ Technology Stack

- **Database**: PostgreSQL 15
- **ETL**: Pandas, SQLAlchemy
- **Orchestration**: Apache Airflow
- **Streaming**: Simulated Kafka (stdin/stdout)
- **Data Generation**: Faker library
- **Containerization**: Docker Compose
- **Monitoring**: Python logging

## 📈 Sample Queries

### Top Performing Games
```sql
SELECT 
    game_name,
    SUM(total_revenue) as total_revenue,
    SUM(total_transactions) as total_transactions,
    ROUND(SUM(total_revenue) / SUM(total_transactions), 2) as avg_revenue_per_txn
FROM v_daily_game_report
GROUP BY game_name
ORDER BY total_revenue DESC
LIMIT 5;
```

### Regional Analysis
```sql
SELECT 
    country_name,
    region,
    SUM(total_revenue) as total_revenue,
    SUM(unique_users) as unique_users
FROM v_daily_game_report
GROUP BY country_name, region
ORDER BY total_revenue DESC;
```

## 🚧 Next Steps & Production Considerations

### Immediate Enhancements
1. **Real Kafka Integration**: Replace stdin/stdout with actual Kafka brokers
   - Connect to Kafka cluster with proper authentication
   - Implement consumer groups and partitioning
   - Add schema registry for Avro/Protobuf support
2. **Error Handling**: Implement dead-letter queues and retry mechanisms
3. **Monitoring**: Add Prometheus metrics and Grafana dashboards
4. **Testing**: Comprehensive unit and integration tests

### Cloud Data Warehouse Integration
1. **Snowflake/Databricks Integration**:
   - Export processed data as Parquet files to S3/MinIO
   - Use Snowpipe/AutoLoader for continuous ingestion
   - Implement incremental loading with change data capture
   - Set up data quality monitoring and alerting
2. **Data Lake Architecture**:
   - Bronze layer: Raw data in cloud storage
   - Silver layer: Cleaned and validated data  
   - Gold layer: Aggregated business-ready data
   - Implement data governance and cataloging

### Advanced Features
1. **Cloud Deployment**: Dockerize entire pipeline for cloud deployment
2. **Data Lake Integration**: Add S3/MinIO for raw data storage
3. **dbt Transformation**: Implement dbt for transformation layer
4. **Snowflake/Databricks**: Add cloud data warehouse integration
5. **CI/CD Pipeline**: Automated testing and deployment

### Production Ready
1. **Configuration Management**: Use environment-specific configs
2. **Secret Management**: Integrate with HashiCorp Vault or AWS Secrets Manager
3. **Scalability**: Horizontal scaling for high-volume data
4. **Backup & Recovery**: Database backups and disaster recovery plans
5. **Compliance**: GDPR, CCPA, and data privacy compliance

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For questions or issues:
1. Check the troubleshooting section below
2. Review the code comments and documentation
3. Open an issue on GitHub with detailed information

## 🔍 Troubleshooting

### Common Issues

**Database Connection Errors**
- Verify Docker containers are running: `docker ps`
- Check database credentials in `.env` file
- Ensure port 5432 is available

**Python Package Issues**
- Use virtual environment: `python -m venv venv`
- Update pip: `pip install --upgrade pip`
- Check Python version: `python --version` (requires 3.10+)

**File Path Issues**
- Use absolute paths or relative paths from project root
- Ensure data directory exists: `mkdir -p data`

### Logs and Debugging
- Check `pipeline.log` for detailed error messages
- Enable debug logging by changing log level in utils.py
- Use `--help` flag on any script for usage information

---

**Happy data engineering! 🚀**
