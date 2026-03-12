import os
import io
import logging
from datetime import datetime
import pandas as pd

from minio import Minio
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load .env if running locally; Docker usually injects these automatically
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class MinioLakehouseETL:
    def __init__(self):
        # 1. MinIO Connection - Use ROOT_USER from your .env
        # If running in Docker, use 'minio:9000'. If local, use 'localhost:9000'.
        self.minio_client = Minio(
            os.getenv('MINIO_ENDPOINT', 'minio:9000'), 
            access_key=os.getenv('MINIO_ROOT_USER', 'admin'),
            secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'password'),
            secure=False
        )
        self.bucket_name = 'input-data'
        
        # 2. PostgreSQL Connection - Matches your .env postgres_user
        self.pg_conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'lakehouse_db'),
            user=os.getenv('POSTGRES_USER', 'postgres_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres_password'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )
        self.pg_conn.autocommit = True
        self.schema = "public"
        self.log_table = "minio_ingestion_log"

    def setup_infrastructure(self):
        """Ensures MinIO bucket and Postgres log tables exist."""
        if not self.minio_client.bucket_exists(self.bucket_name):
            self.minio_client.make_bucket(self.bucket_name)
            logger.info(f"Created MinIO bucket: {self.bucket_name}")

        with self.pg_conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{self.log_table} (
                    file_name TEXT PRIMARY KEY,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            logger.info("Database infrastructure ready.")

    def is_already_ingested(self, object_name):
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM {self.schema}.{self.log_table} WHERE file_name = %s;", (object_name,))
            return cursor.fetchone() is not None

    def read_csv_from_minio(self, object_name):
        try:
            response = self.minio_client.get_object(self.bucket_name, object_name)
            # Use BytesIO to handle the stream from MinIO
            data = response.read()
            df = pd.read_csv(io.BytesIO(data))
            return df
        except Exception as e:
            logger.error(f"Error reading {object_name}: {e}")
            return None
        finally:
            if 'response' in locals():
                response.close()
                response.release_conn()

    def sync_table_schema(self, df, table_name):
        """Dynamically adds columns as TEXT to handle varying CSV structures."""
        with self.pg_conn.cursor() as cursor:
            cols_def = ", ".join([f'"{col}" TEXT' for col in df.columns])
            cursor.execute(f'CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} ({cols_def});')

            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s;
            """, (self.schema, table_name))
            
            existing_cols = {row[0].lower() for row in cursor.fetchall()}
            for col in df.columns:
                if col.lower() not in existing_cols:
                    cursor.execute(f'ALTER TABLE {self.schema}.{table_name} ADD COLUMN "{col}" TEXT;')
                    logger.info(f"Added missing column '{col}' to {table_name}")

    def load_to_postgres(self, df, table_name, object_name):
        try:
            # Standardize column names to lowercase
            df.columns = df.columns.str.lower()
            df['source_filename'] = object_name
            
            self.sync_table_schema(df, table_name)

            with self.pg_conn.cursor() as cursor:
                cols = ','.join([f'"{c}"' for c in df.columns])
                values = [tuple(x) for x in df.to_numpy()]
                insert_query = f"INSERT INTO {self.schema}.{table_name} ({cols}) VALUES %s"
                execute_values(cursor, insert_query, values)
                
                cursor.execute(
                    f"INSERT INTO {self.schema}.{self.log_table} (file_name) VALUES (%s) ON CONFLICT DO NOTHING;",
                    (object_name,)
                )
            logger.info(f"✅ Successfully loaded {len(df)} rows from {object_name}")
        except Exception as e:
            logger.error(f"❌ Failed to load {object_name}: {e}")

    def run_pipeline(self, target_table, prefix=""):
        """
        prefix="": Looks at the root of the bucket.
        prefix="raw/": Looks inside the 'raw' folder.
        """
        logger.info("🚀 Starting MinIO Lakehouse ETL Pipeline")
        self.setup_infrastructure()

        objects = self.minio_client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
        
        found_files = False
        for obj in objects:
            if obj.object_name.endswith('.csv'):
                found_files = True
                if self.is_already_ingested(obj.object_name):
                    logger.info(f"⏭️ Skipping {obj.object_name} (already ingested)")
                    continue

                df = self.read_csv_from_minio(obj.object_name)
                if df is not None and not df.empty:
                    self.load_to_postgres(df, target_table, obj.object_name)
        
        if not found_files:
            logger.warning(f"No CSV files found in bucket '{self.bucket_name}' with prefix '{prefix}'")

    def close(self):
        if hasattr(self, 'pg_conn') and self.pg_conn:
            self.pg_conn.close()

if __name__ == "__main__":
    etl = MinioLakehouseETL()
    try:
        # Set prefix to "" if your files are in the root of 'input-data'
        etl.run_pipeline(target_table="raw_wwtw_data", prefix="")
    finally:
        etl.close()