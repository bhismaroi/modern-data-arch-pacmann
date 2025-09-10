#!/usr/bin/env python3
"""
Database initialization script for Modern Data Architecture project.
Creates tables and inserts initial dummy data.
"""

import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime, timedelta
import random
import sys

# Database configuration from environment variables
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'datawarehouse')
DB_USER = os.getenv('POSTGRES_USER', 'user')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

def create_database():
    """Create database if it doesn't exist."""
    try:
        # Connect to PostgreSQL server
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"Database '{DB_NAME}' created successfully")
        else:
            print(f"Database '{DB_NAME}' already exists")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error creating database: {e}")
        return False

def create_tables():
    """Create necessary tables in the database."""
    try:
        # Connect to the datawarehouse database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        
        # Create sales_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales_data (
                id SERIAL PRIMARY KEY,
                transaction_date DATE NOT NULL,
                product_id VARCHAR(50) NOT NULL,
                product_name VARCHAR(200) NOT NULL,
                category VARCHAR(100) NOT NULL,
                subcategory VARCHAR(100),
                quantity INTEGER NOT NULL,
                unit_price DECIMAL(10, 2) NOT NULL,
                total_amount DECIMAL(10, 2) NOT NULL,
                customer_id VARCHAR(50) NOT NULL,
                customer_name VARCHAR(200),
                customer_segment VARCHAR(50),
                region VARCHAR(100),
                city VARCHAR(100),
                payment_method VARCHAR(50),
                discount_percentage DECIMAL(5, 2) DEFAULT 0,
                profit_margin DECIMAL(5, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create model_metadata table for ML model tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS model_metadata (
                id SERIAL PRIMARY KEY,
                model_name VARCHAR(100) NOT NULL,
                model_version VARCHAR(50) NOT NULL,
                training_date TIMESTAMP NOT NULL,
                model_type VARCHAR(50) NOT NULL,
                accuracy DECIMAL(5, 4),
                precision_score DECIMAL(5, 4),
                recall_score DECIMAL(5, 4),
                f1_score DECIMAL(5, 4),
                mse DECIMAL(10, 4),
                rmse DECIMAL(10, 4),
                r2_score DECIMAL(5, 4),
                training_samples INTEGER,
                test_samples INTEGER,
                features_used TEXT,
                model_path VARCHAR(500),
                hyperparameters JSONB,
                training_duration_seconds DECIMAL(10, 2),
                created_by VARCHAR(100) DEFAULT 'airflow',
                is_active BOOLEAN DEFAULT FALSE,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create data_pipeline_logs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_pipeline_logs (
                id SERIAL PRIMARY KEY,
                pipeline_name VARCHAR(100) NOT NULL,
                run_id VARCHAR(100) UNIQUE NOT NULL,
                status VARCHAR(50) NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                records_processed INTEGER,
                records_failed INTEGER,
                error_message TEXT,
                source_file VARCHAR(500),
                destination_table VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create category_summary table for aggregated data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS category_summary (
                id SERIAL PRIMARY KEY,
                summary_date DATE NOT NULL,
                category VARCHAR(100) NOT NULL,
                total_sales DECIMAL(12, 2),
                total_quantity INTEGER,
                avg_unit_price DECIMAL(10, 2),
                total_customers INTEGER,
                avg_discount DECIMAL(5, 2),
                avg_profit_margin DECIMAL(5, 2),
                top_product VARCHAR(200),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(summary_date, category)
            )
        """)
        
        # Create indexes for better query performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON sales_data(transaction_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_category ON sales_data(category)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_customer ON sales_data(customer_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_model_active ON model_metadata(is_active)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_status ON data_pipeline_logs(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_category_summary_date ON category_summary(summary_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_category_summary_category ON category_summary(category)")
        
        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
        print("Tables and indexes created successfully")
        return True


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Initialize PostgreSQL database and create tables for the Modern Data Architecture project"
    )
    parser.add_argument(
        "--skip-create-db",
        action="store_true",
        help="Skip database creation step (assumes database already exists)"
    )
    args = parser.parse_args()

    if not args.skip_create_db:
        if not create_database():
            sys.exit(1)

    if not create_tables():
        sys.exit(1)

    print("Database initialization completed successfully")


if __name__ == "__main__":
    main()