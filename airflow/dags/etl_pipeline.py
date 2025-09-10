"""
ETL Pipeline DAG for Modern Data Architecture
Extracts data from source, stores in MinIO, transforms, and loads to PostgreSQL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
from minio import Minio
from io import BytesIO
import json
import os
import logging
import uuid

# Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
POSTGRES_CONN_ID = 'postgres_default'

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def extract_data(**context):
    """
    Extract data from source (simulate API or file extraction)
    In production, this would connect to actual data sources
    """
    logging.info("Starting data extraction...")
    
    # Generate synthetic daily sales data
    num_records = np.random.randint(100, 500)
    
    # Product categories and details
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books']
    products = {
        'Electronics': ['Laptop', 'Smartphone', 'Tablet', 'Headphones'],
        'Clothing': ['Shirts', 'Pants', 'Shoes', 'Accessories'],
        'Home & Garden': ['Furniture', 'Kitchen', 'Garden', 'Decor'],
        'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children']
    }
    
    regions = ['North', 'South', 'East', 'West', 'Central']
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash']
    
    data = []
    execution_date = context['execution_date']
    
    for i in range(num_records):
        category = np.random.choice(categories)
        subcategory = np.random.choice(products[category])
        
        # Generate price based on category
        if category == 'Electronics':
            unit_price = round(np.random.uniform(50, 2000), 2)
        elif category == 'Clothing':
            unit_price = round(np.random.uniform(15, 300), 2)
        elif category == 'Home & Garden':
            unit_price = round(np.random.uniform(20, 1000), 2)
        else:  # Books
            unit_price = round(np.random.uniform(10, 100), 2)
        
        quantity = np.random.randint(1, 10)
        discount = np.random.choice([0, 5, 10, 15, 20])
        
        record = {
            'transaction_id': f"TXN-{execution_date.strftime('%Y%m%d')}-{str(i+1).zfill(5)}",
            'transaction_date': execution_date.strftime('%Y-%m-%d'),
            'product_id': f"PRD-{category[:3].upper()}-{str(np.random.randint(1000, 9999))}",
            'product_name': f"{subcategory} {np.random.randint(100, 999)}",
            'category': category,
            'subcategory': subcategory,
            'quantity': quantity,
            'unit_price': unit_price,
            'discount_percentage': discount,
            'customer_id': f"CUST-{str(np.random.randint(1, 1000)).zfill(5)}",
            'region': np.random.choice(regions),
            'payment_method': np.random.choice(payment_methods)
        }
        
        # Calculate total amount
        subtotal = record['unit_price'] * record['quantity']
        discount_amount = subtotal * (record['discount_percentage'] / 100)
        record['total_amount'] = round(subtotal - discount_amount, 2)
        
        data.append(record)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Save to context for next task
    context['task_instance'].xcom_push(key='extracted_data', value=df.to_json())
    
    logging.info(f"Extracted {len(df)} records successfully")
    return f"Extracted {len(df)} records"

def upload_to_minio(**context):
    """
    Upload raw data to MinIO storage
    """
    logging.info("Uploading data to MinIO...")
    
    # Get data from previous task
    data_json = context['task_instance'].xcom_pull(task_ids='extract_data', key='extracted_data')
    df = pd.read_json(data_json)
    
    # Generate file name with timestamp
    execution_date = context['execution_date']
    file_name = f"sales_data_{execution_date.strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Convert DataFrame to CSV in memory
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Upload to MinIO
    try:
        # Ensure bucket exists
        if not minio_client.bucket_exists('raw-data'):
            minio_client.make_bucket('raw-data')
        
        # Upload file
        minio_client.put_object(
            'raw-data',
            file_name,
            csv_buffer,
            length=len(csv_buffer.getvalue()),
            content_type='text/csv'
        )
        
        # Store file path for next task
        context['task_instance'].xcom_push(key='minio_file_path', value=file_name)
        
        logging.info(f"Successfully uploaded {file_name} to MinIO")
        return f"Uploaded {file_name}"
        
    except Exception as e:
        logging.error(f"Error uploading to MinIO: {str(e)}")
        raise

def transform_data(**context):
    """
    Read data from MinIO, apply transformations, and prepare for loading
    """
    logging.info("Starting data transformation...")
    
    # Get file path from previous task
    file_name = context['task_instance'].xcom_pull(task_ids='upload_to_minio', key='minio_file_path')
    
    try:
        # Download from MinIO
        response = minio_client.get_object('raw-data', file_name)
        df = pd.read_csv(BytesIO(response.data))
        
        # Apply transformations
        # 1. Add customer segment based on total amount
        def categorize_customer(amount):
            if amount >= 1000:
                return 'Premium'
            elif amount >= 500:
                return 'Standard'
            else:
                return 'Budget'
        
        df['customer_segment'] = df['total_amount'].apply(categorize_customer)
        
        # 2. Add profit margin (random between 10-40%)
        df['profit_margin'] = np.random.uniform(10, 40, len(df)).round(2)
        
        # 3. Add customer name (simulated)
        df['customer_name'] = df['customer_id'].apply(lambda x: f"Customer {x[-4:]}")
        
        # 4. Add city based on region
        region_cities = {
            'North': ['New York', 'Boston', 'Chicago'],
            'South': ['Houston', 'Miami', 'Atlanta'],
            'East': ['Philadelphia', 'Washington', 'Baltimore'],
            'West': ['Los Angeles', 'San Francisco', 'Seattle'],
            'Central': ['Dallas', 'Denver', 'Kansas City']
        }
        df['city'] = df['region'].apply(lambda x: np.random.choice(region_cities.get(x, ['Unknown'])))
        
        # 5. Data quality checks
        df = df.dropna()  # Remove any null values
        df = df[df['total_amount'] > 0]  # Remove negative or zero amounts
        
        # Upload transformed data to processed-data bucket
        transformed_file_name = f"processed_{file_name}"
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        if not minio_client.bucket_exists('processed-data'):
            minio_client.make_bucket('processed-data')
        
        minio_client.put_object(
            'processed-data',
            transformed_file_name,
            csv_buffer,
            length=len(csv_buffer.getvalue()),
            content_type='text/csv'
        )
        
        # Pass transformed data to next task
        context['task_instance'].xcom_push(key='transformed_data', value=df.to_json())
        context['task_instance'].xcom_push(key='transformed_file', value=transformed_file_name)
        
        logging.info(f"Transformed {len(df)} records successfully")
        return f"Transformed {len(df)} records"
        
    except Exception as e:
        logging.error(f"Error in transformation: {str(e)}")
        raise

def load_to_postgres(**context):
    """
    Load transformed data to PostgreSQL
    """
    logging.info("Loading data to PostgreSQL...")
    
    # Get transformed data
    data_json = context['task_instance'].xcom_pull(task_ids='transform_data', key='transformed_data')
    df = pd.read_json(data_json)
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            records.append((
                row['transaction_date'],
                row['product_id'],
                row['product_name'],
                row['category'],
                row['subcategory'],
                int(row['quantity']),
                float(row['unit_price']),
                float(row['total_amount']),
                row['customer_id'],
                row['customer_name'],
                row['customer_segment'],
                row['region'],
                row['city'],
                row['payment_method'],
                float(row['discount_percentage']),
                float(row['profit_margin'])
            ))
        
        # Insert data
        insert_query = """
            INSERT INTO sales_data (
                transaction_date, product_id, product_name, category, subcategory,
                quantity, unit_price, total_amount, customer_id, customer_name,
                customer_segment, region, city, payment_method, discount_percentage,
                profit_margin
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(insert_query, records)
        
        # Log pipeline execution
        execution_date = context['execution_date']
        run_id = f"etl_{execution_date.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        source_file = context['task_instance'].xcom_pull(task_ids='upload_to_minio', key='minio_file_path')
        
        cursor.execute("""
            INSERT INTO data_pipeline_logs (
                pipeline_name, run_id, status, start_time, end_time,
                records_processed, records_failed, source_file, destination_table
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            'etl_pipeline',
            run_id,
            'SUCCESS',
            context['dag_run'].start_date,
            datetime.now(),
            len(df),
            0,
            f'/raw-data/{source_file}',
            'sales_data'
        ))
        
        conn.commit()
        logging.info(f"Successfully loaded {len(df)} records to PostgreSQL")
        
        return f"Loaded {len(df)} records to PostgreSQL"
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading to PostgreSQL: {str(e)}")
        
        # Log failure
        cursor.execute("""
            INSERT INTO data_pipeline_logs (
                pipeline_name, run_id, status, start_time, end_time,
                records_processed, records_failed, error_message, destination_table
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            'etl_pipeline',
            f"etl_{context['execution_date'].strftime('%Y%m%d_%H%M%S')}_failed",
            'FAILED',
            context['dag_run'].start_date,
            datetime.now(),
            0,
            len(df),
            str(e),
            'sales_data'
        ))
        conn.commit()
        raise
        
    finally:
        cursor.close()
        conn.close()

def update_category_summary(**context):
    """
    Update category summary table with aggregated data
    """
    logging.info("Updating category summary...")
    
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        # Calculate category summaries for the day
        cursor.execute("""
            INSERT INTO category_summary (
                summary_date, category, total_sales, total_quantity,
                avg_unit_price, total_customers, avg_discount, avg_profit_margin
            )
            SELECT 
                %s as summary_date,
                category,
                SUM(total_amount) as total_sales,
                SUM(quantity) as total_quantity,
                AVG(unit_price) as avg_unit_price,
                COUNT(DISTINCT customer_id) as total_customers,
                AVG(discount_percentage) as avg_discount,
                AVG(profit_margin) as avg_profit_margin
            FROM sales_data
            WHERE DATE(transaction_date) = %s
            GROUP BY category
            ON CONFLICT (summary_date, category) 
            DO UPDATE SET
                total_sales = EXCLUDED.total_sales,
                total_quantity = EXCLUDED.total_quantity,
                avg_unit_price = EXCLUDED.avg_unit_price,
                total_customers = EXCLUDED.total_customers,
                avg_discount = EXCLUDED.avg_discount,
                avg_profit_margin = EXCLUDED.avg_profit_margin,
                created_at = CURRENT_TIMESTAMP
        """, (execution_date, execution_date))
        
        conn.commit()
        logging.info("Category summary updated successfully")
        return "Category summary updated"
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error updating category summary: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

# Default DAG arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for sales data processing',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['etl', 'sales', 'daily']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_to_minio,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag
)

summary_task = PythonOperator(
    task_id='update_category_summary',
    python_callable=update_category_summary,
    dag=dag
)

# Create PostgreSQL connection
create_conn = PostgresOperator(
    task_id='create_postgres_conn',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
        SELECT 1;
    """,
    dag=dag,
    trigger_rule='dummy'
)

# Set task dependencies
extract_task >> upload_task >> transform_task >> load_task >> summary_task