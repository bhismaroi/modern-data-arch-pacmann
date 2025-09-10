"""
Machine Learning Model Retraining Pipeline
Retrains the sales prediction model on recent data, stores artefacts in MinIO, and logs metadata in Postgres.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
import os
import tempfile

import pandas as pd
from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from minio import Minio

# Local project import
from ml.train_model import SalesPredictionModel

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_MODEL_BUCKET", "models")
MODEL_PREFIX = os.getenv("MODEL_OBJECT_PREFIX", "sales_predictor/")
MODEL_TYPE = os.getenv("MODEL_TYPE", "linear_regression")
RETRAIN_LOOKBACK_DAYS = int(os.getenv("RETRAIN_LOOKBACK_DAYS", "90"))
MIN_RECORDS = int(os.getenv("MIN_RETRAIN_RECORDS", "100"))

# -----------------------------------------------------------------------------
# MinIO client (bucket ensured at DAG-parse time)
# -----------------------------------------------------------------------------
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)
if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)


# -----------------------------------------------------------------------------
# Task functions
# -----------------------------------------------------------------------------

def fetch_training_data(**context):
    """Fetch recent sales data; fallback to full table if not enough rows."""
    logging.info("Fetching training data from Postgres …")
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()

    query_recent = f"""
        SELECT *
        FROM sales_data
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '{RETRAIN_LOOKBACK_DAYS} days'
        ORDER BY transaction_date DESC;
    """
    df = pd.read_sql(query_recent, conn)

    if len(df) < MIN_RECORDS:
        logging.warning("Only %s recent rows—using full table", len(df))
        df = pd.read_sql("SELECT * FROM sales_data", conn)

    temp_dir = tempfile.mkdtemp(prefix="retrain_")
    data_path = os.path.join(temp_dir, "training.csv")
    df.to_csv(data_path, index=False)
    logging.info("Saved %s rows to %s", len(df), data_path)
    return data_path


def train_and_save_model(data_path: str, **context):
    """Train model and persist artefacts locally."""
    logging.info("Training model with data at %s", data_path)
    df = pd.read_csv(data_path)

    model = SalesPredictionModel(model_type=MODEL_TYPE)
    model.load_data(df=df)
    model.train()

    artefact_dir = tempfile.mkdtemp(prefix="model_")
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    model_filename = f"sales_predictor_{ts}.joblib"
    model_path = os.path.join(artefact_dir, model_filename)
    model.save_model(filepath=model_path)

    metrics_path = model_path.replace(".joblib", "_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(model.metrics, f, indent=2)

    return {
        "model_path": model_path,
        "metrics_path": metrics_path,
        "model_filename": model_filename,
        "metrics": model.metrics,
    }


def upload_to_minio(model_info: dict, **context):
    """Upload artefacts to MinIO bucket and return object names."""
    model_obj = f"{MODEL_PREFIX}{model_info['model_filename']}"
    metrics_obj = model_obj.replace(".joblib", "_metrics.json")

    minio_client.fput_object(MINIO_BUCKET, model_obj, model_info["model_path"])
    minio_client.fput_object(MINIO_BUCKET, metrics_obj, model_info["metrics_path"])
    logging.info("Uploaded model artefacts to MinIO: %s", model_obj)

    return {"model_object": model_obj, "metrics_object": metrics_obj}


def log_model_metadata(model_info: dict, minio_objects: dict, **context):
    """Insert metadata row and deactivate previous active models."""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("UPDATE model_metadata SET is_active = FALSE WHERE is_active IS TRUE")

    metrics = model_info["metrics"]
    cursor.execute(
        """
        INSERT INTO model_metadata (
            model_name, model_version, training_date, model_type,
            mse, rmse, r2_score,
            training_samples, test_samples, features_used,
            model_path, hyperparameters, created_by, is_active, notes
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            "sales_predictor",
            datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
            datetime.utcnow(),
            MODEL_TYPE,
            metrics.get("test_mse"),
            metrics.get("test_rmse"),
            metrics.get("test_r2"),
            metrics.get("training_samples"),
            metrics.get("test_samples"),
            json.dumps(metrics.get("features_used", [])),
            f"s3://{MINIO_BUCKET}/{minio_objects['model_object']}",
            json.dumps({}),
            "airflow",
            True,
            json.dumps(metrics),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Model metadata logged.")


# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=10)}

dag = DAG(
    dag_id="ml_retraining",
    description="Retrain sales predictor model and log artefacts",
    start_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ml", "retrain"],
)

with dag:
    t_fetch = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_training_data,
    )

    t_train = PythonOperator(
        task_id="train_model",
        python_callable=train_and_save_model,
        op_kwargs={"data_path": XComArg(t_fetch)},
    )

    t_upload = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
        op_kwargs={"model_info": XComArg(t_train)},
    )

    t_log = PythonOperator(
        task_id="log_metadata",
        python_callable=log_model_metadata,
        op_kwargs={
            "model_info": XComArg(t_train),
            "minio_objects": XComArg(t_upload),
        },
    )

    t_fetch >> t_train >> t_upload >> t_log
Retrains the sales prediction model on recent data and stores the artefacts in MinIO.
Also logs model metadata to Postgres so that it can be surfaced in Metabase.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
import os
import tempfile
from io import BytesIO

import joblib
import pandas as pd
from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from minio import Minio

# Import training util from project package
from ml.train_model import SalesPredictionModel

# -----------------------------------------------------------------------------
# Configuration via environment variables with sane defaults
# -----------------------------------------------------------------------------
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_MODEL_BUCKET", "models")
MODEL_PREFIX = os.getenv("MODEL_OBJECT_PREFIX", "sales_predictor/")
MODEL_TYPE = os.getenv("MODEL_TYPE", "linear_regression")
RETRAIN_LOOKBACK_DAYS = int(os.getenv("RETRAIN_LOOKBACK_DAYS", "90"))
MIN_RECORDS = int(os.getenv("MIN_RETRAIN_RECORDS", "100"))

# -----------------------------------------------------------------------------
# Initialise shared clients (safe because Airflow forks processes for tasks)
# -----------------------------------------------------------------------------
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

# Ensure bucket exists at DAG parse time so tasks don't race
if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)


# -----------------------------------------------------------------------------
# Task functions
# -----------------------------------------------------------------------------

def fetch_training_data(**context):
    """Fetch recent sales data from Postgres, fall back to all data if insufficient."""
    logging.info("Fetching training data from Postgres…")
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()

    query_recent = f"""
        SELECT *
        FROM sales_data
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '{RETRAIN_LOOKBACK_DAYS} days'
        ORDER BY transaction_date DESC;
    """
    df = pd.read_sql(query_recent, conn)

    if len(df) < MIN_RECORDS:
        logging.warning(
            "Only %s records found in last %s days, falling back to full table",
            len(df),
            RETRAIN_LOOKBACK_DAYS,
        )
        df = pd.read_sql("SELECT * FROM sales_data", conn)

    # Persist to a temporary CSV so we don't blow up XCom size limits
    temp_dir = tempfile.mkdtemp(prefix="retrain_data_")
    data_path = os.path.join(temp_dir, "training_data.csv")
    df.to_csv(data_path, index=False)
    logging.info("Saved training data to %s (%s rows)", data_path, len(df))
    return data_path



def train_and_save_model(data_path: str, **context):
    """Train the model using project utilities and save artefacts locally."""
    logging.info("Loading data from %s", data_path)
    df = pd.read_csv(data_path)

    model = SalesPredictionModel(model_type=MODEL_TYPE)
    model.load_data(df=df)
    model.train()

    # Persist artefacts to a temporary directory
    artefact_dir = tempfile.mkdtemp(prefix="model_")
    ts_suffix = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    model_filename = f"sales_predictor_{ts_suffix}.joblib"
    model_path = os.path.join(artefact_dir, model_filename)
    model.save_model(filepath=model_path)

    # Save metrics separately for convenience
    metrics_path = model_path.replace(".joblib", "_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(model.metrics, f, indent=2)

    logging.info("Model artefacts saved under %s", artefact_dir)
    return {
        "model_path": model_path,
        "metrics_path": metrics_path,
        "metrics": model.metrics,
        "model_filename": model_filename,
    }


def upload_to_minio(model_info: dict, **context):
    """Upload model artefacts to MinIO and return the object URI."""
    model_path = model_info["model_path"]
    metrics_path = model_info["metrics_path"]
    model_filename = model_info["model_filename"]

    object_name = f"{MODEL_PREFIX}{model_filename}"
    logging.info("Uploading %s to MinIO bucket %s as %s", model_path, MINIO_BUCKET, object_name)

    # Upload model binary
    minio_client.fput_object(MINIO_BUCKET, object_name, model_path)

    # Upload metrics json
    metrics_object_name = object_name.replace(".joblib", "_metrics.json")
    minio_client.fput_object(MINIO_BUCKET, metrics_object_name, metrics_path)

    # Return object paths for downstream task
    return {
        "model_object": object_name,
        "metrics_object": metrics_object_name,
    }


def log_model_metadata(model_info: dict, minio_objects: dict, **context):
    """Insert a record for the new model and deactivate previous ones."""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Deactivate previous models
    cursor.execute("UPDATE model_metadata SET is_active = FALSE WHERE is_active IS TRUE")

    metrics = model_info["metrics"]
    insert_sql = """
        INSERT INTO model_metadata (
            model_name, model_version, training_date, model_type,
            accuracy, precision_score, recall_score, f1_score,
            mse, rmse, r2_score,
            training_samples, test_samples, features_used,
            model_path, hyperparameters, training_duration_seconds,
            created_by, is_active, notes
        ) VALUES (
            %(model_name)s, %(model_version)s, %(training_date)s, %(model_type)s,
            NULL, NULL, NULL, NULL,
            %(mse)s, %(rmse)s, %(r2)s,
            %(train_samples)s, %(test_samples)s, %(features_used)s,
            %(model_path)s, %(hyperparameters)s, %(train_dur)s,
            'airflow', TRUE, %(notes)s
        );
    """
    cursor.execute(
        insert_sql,
        {
            "model_name": "sales_predictor",
            "model_version": datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
            "training_date": datetime.utcnow(),
            "model_type": MODEL_TYPE,
            "mse": metrics.get("test_mse"),
            "rmse": metrics.get("test_rmse"),
            "r2": metrics.get("test_r2"),
            "train_samples": metrics.get("training_samples"),
            "test_samples": metrics.get("test_samples"),
            "features_used": json.dumps(metrics.get("features_used", [])),
            "model_path": f"s3://{MINIO_BUCKET}/{minio_objects['model_object']}",
            "hyperparameters": json.dumps({}),
            "train_dur": None,
            "notes": json.dumps(metrics),
        },
    )
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Model metadata logged to Postgres")


# -----------------------------------------------------------------------------
# DAG definition
# -----------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="ml_retraining",
    description="Retrain sales prediction model and store artefacts",
    start_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ml", "retrain"],
)

with dag:
    fetch_data_task = PythonOperator(
        task_id="fetch_training_data",
        python_callable=fetch_training_data,
        provide_context=True,
    )

    train_model_task = PythonOperator(
        task_id="train_and_save_model",
        python_callable=train_and_save_model,
        op_kwargs={"data_path": XComArg(fetch_data_task)},
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
        op_kwargs={"model_info": XComArg(train_model_task)},
        provide_context=True,
    )

    log_metadata_task = PythonOperator(
        task_id="log_model_metadata",
        python_callable=log_model_metadata,
        op_kwargs={
            "model_info": XComArg(train_model_task),
            "minio_objects": XComArg(upload_task),
        },
        provide_context=True,
    )

    fetch_data_task >> train_model_task >> upload_task >> log_metadata_task")
    
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    
    try:
        # Fetch sales data from the last 90 days
        query = """
            SELECT 
                transaction_date,
                category,
                subcategory,
                quantity,
                unit_price,
                discount_percentage,
                total_amount,
                customer_segment,
                region,
                payment_method,
                profit_margin
            FROM sales_data
            WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
            ORDER BY transaction_date DESC
        """
        
        df = pd.read_sql(query, conn)
        
        if len(df) < 100:
            # If not enough recent data, fetch all data
            query_all = """
                SELECT 
                    transaction_date,
                    category,
                    subcategory,
                    quantity,