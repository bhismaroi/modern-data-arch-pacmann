# Modern Data Architecture Project

A complete data pipeline solution featuring data ingestion, storage, transformation, analytics, and machine learning capabilities.

## Architecture Overview

```
Data Source → MinIO (Raw Storage) → Airflow (Orchestration) → PostgreSQL (Processed Data) → Metabase (Analytics)
                                           ↓
                                    ML Model Training → MinIO (Model Storage)
```

## 📋 Prerequisites

- Docker & Docker Compose installed
- Python 3.8+
- Minimum 8GB RAM available
- 10GB free disk space

## 🚀 Quick Start

1. **Clone the repository**
```bash
git clone https://github.com/bhismaroi/modern-data-arch-pacmann
cd modern-data-arch-pacmann
```

2. **Set up environment variables**
```bash
cp .env.example .env
# Edit .env file with your configurations if needed
```

3. **Start all services**
```bash
docker-compose up -d
```

4. **Initialize the database**
```bash
docker exec -it postgres-db python /scripts/init_db.py
```

5. **Access the services**
- **Airflow**: http://localhost:8080 (admin/admin)
- **MinIO**: http://localhost:9001 (minioadmin/minioadmin)
- **Metabase**: http://localhost:3000
- **PostgreSQL**: localhost:5432 (user/password)

## 📁 Project Structure

```
modern-data-architecture/
├── docker-compose.yml          # All services configuration
├── .env.example               # Environment variables template
├── README.md                  # This file
├── airflow/
│   ├── dags/
│   │   ├── etl_pipeline.py   # Main ETL pipeline
│   │   └── ml_retraining.py  # ML model retraining pipeline
│   ├── plugins/
│   └── requirements.txt
├── scripts/
│   ├── init_db.py           # Database initialization
│   ├── generate_dummy_data.py # Generate sample data
│   └── utils.py             # Helper functions
├── data/
│   └── sample_sales.csv     # Sample data file
├── ml/
│   ├── train_model.py       # Model training script
│   └── model_utils.py       # ML utilities
└── metabase/
    └── dashboards/          # Dashboard configurations
```

## 🔧 Components

### 1. MinIO (Object Storage)
- **Purpose**: Store raw data files and ML models
- **Buckets**:
  - `raw-data`: Original data files
  - `processed-data`: Transformed data
  - `ml-models`: Trained models

### 2. PostgreSQL (Data Warehouse)
- **Purpose**: Store structured, processed data
- **Databases**:
  - `datawarehouse`: Main analytical database
- **Tables**:
  - `sales_data`: Processed sales information
  - `model_metadata`: ML model training history

### 3. Apache Airflow (Orchestration)
- **Purpose**: Schedule and monitor workflows
- **DAGs**:
  - `etl_pipeline`: Daily data extraction, transformation, and loading
  - `ml_retraining`: Weekly model retraining

### 4. Metabase (Business Intelligence)
- **Purpose**: Data visualization and analytics
- **Dashboards**:
  - Sales Overview
  - Category Performance
  - ML Model Performance

## 📊 Data Pipeline Workflows

### ETL Pipeline (runs daily at 2 AM)
1. Extract data from source (CSV/API)
2. Upload raw data to MinIO
3. Read and transform data
4. Load processed data to PostgreSQL
5. Update metadata

### ML Retraining Pipeline (runs weekly on Sundays)
1. Fetch latest data from PostgreSQL
2. Train linear regression model
3. Evaluate model performance
4. Store model in MinIO
5. Log metadata to PostgreSQL

## 🛠️ Development

### Adding New Data Sources
1. Create extraction script in `scripts/`
2. Update ETL DAG in `airflow/dags/etl_pipeline.py`
3. Add transformation logic
4. Update database schema if needed

### Creating New Dashboards
1. Access Metabase at http://localhost:3000
2. Connect to PostgreSQL database
3. Create queries and visualizations
4. Save dashboard configuration

### Training Different Models
1. Modify `ml/train_model.py`
2. Update the retraining DAG
3. Adjust model storage logic

## 📝 Configuration

### Environment Variables
See `.env.example` for all available configurations:
- Database credentials
- Service ports
- Storage paths
- Airflow settings

### Scaling
- Adjust resource limits in `docker-compose.yml`
- Configure parallel task execution in Airflow
- Add PostgreSQL read replicas for heavy analytics

## 🧪 Testing

### Generate Sample Data
```bash
python scripts/generate_dummy_data.py --records 10000
```

### Test ETL Pipeline
```bash
docker exec -it airflow-webserver airflow dags test etl_pipeline
```

### Test ML Pipeline
```bash
docker exec -it airflow-webserver airflow dags test ml_retraining
```

## 🐛 Troubleshooting

### Common Issues

1. **Services not starting**
   - Check Docker daemon is running
   - Verify port availability
   - Review logs: `docker-compose logs [service-name]`

2. **Database connection errors**
   - Verify PostgreSQL is healthy: `docker ps`
   - Check credentials in `.env`
   - Test connection: `docker exec -it postgres-db psql -U user -d datawarehouse`

3. **Airflow DAGs not appearing**
   - Wait 30 seconds for DAG discovery
   - Check DAG syntax: `python airflow/dags/[dag-file].py`
   - Review Airflow logs

4. **MinIO access issues**
   - Verify buckets are created
   - Check credentials
   - Access MinIO console at http://localhost:9001

## 📈 Monitoring

- **Airflow**: Monitor DAG runs and task logs
- **MinIO**: Track storage usage and object access
- **PostgreSQL**: Query performance and connection stats
- **Docker**: `docker stats` for resource usage

## 🔒 Security Notes

⚠️ **For local development only!** 

For production:
- Change all default passwords
- Enable SSL/TLS
- Implement proper authentication
- Set up network isolation
- Enable audit logging

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [MinIO Documentation](https://docs.min.io/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Metabase Documentation](https://www.metabase.com/docs/)
