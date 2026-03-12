import os
import io
import logging
from datetime import datetime

import pandas as pd
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient
import psycopg2
from psycopg2.extras import execute_values

# -----------------------------
# CONFIGURATION
# -----------------------------
ACCOUNT_URL = "https://euwseautilcompptdevst.blob.core.windows.net"
CONTAINER_NAME = "data-input"
BLOB_PREFIX = ""  # Optional folder path prefix

PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "wwtw_db"
PG_USER = "postgres"
PG_PASSWORD = os.environ.get("PGPASSWORD")  # Secure in production
PG_SCHEMA = "public"
PG_TABLE = "wwtw_data"
LOG_TABLE = "wwtw_ingest_log"

# -----------------------------
# SETUP LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------
# DATABASE OPERATIONS
# -----------------------------
def initialize_database(cursor, schema, target_table, log_table):
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{target_table} (
            id SERIAL PRIMARY KEY,
            source_file TEXT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{log_table} (
            file_name TEXT PRIMARY KEY,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cursor.connection.commit()
    logging.info(f"Database schema and tables ensured.")


def table_exists(cursor, schema, table):
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        );
    """, (schema, table))
    return cursor.fetchone()[0]


def create_table_with_columns(cursor, schema, table, columns):
    cols_with_types = ", ".join([f"{col} TEXT" for col in columns])
    create_stmt = f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({cols_with_types});"
    cursor.execute(create_stmt)
    cursor.connection.commit()
    logging.info(f"Table {schema}.{table} created or exists with correct columns.")


def is_already_ingested(cursor, schema, log_table, file_name):
    cursor.execute(f"SELECT 1 FROM {schema}.{log_table} WHERE file_name = %s;", (file_name,))
    return cursor.fetchone() is not None


def log_ingestion(cursor, schema, log_table, file_name):
    cursor.execute(
        f"INSERT INTO {schema}.{log_table} (file_name, ingested_at) VALUES (%s, %s) ON CONFLICT (file_name) DO NOTHING;",
        (file_name, datetime.now())
    )
    cursor.connection.commit()
    logging.info(f"Ingestion logged for file '{file_name}'.")


# -----------------------------
# AZURE BLOB STORAGE
# -----------------------------
def authenticate_azure(account_url):
    logging.info("Authenticating with Azure via CLI credentials...")
    credential = AzureCliCredential()
    return BlobServiceClient(account_url=account_url, credential=credential)


def list_blobs(container_client, prefix):
    logging.info(f"Listing blobs in container '{container_client.container_name}' with prefix '{prefix}'...")
    return container_client.list_blobs(name_starts_with=prefix)


def download_blob(container_client, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    stream = blob_client.download_blob()
    data_bytes = stream.readall()
    data_str = io.StringIO(data_bytes.decode('utf-8'))
    return pd.read_csv(data_str)


# -----------------------------
# MAIN INGESTION LOGIC
# -----------------------------
def ingest_blobs(blob_list, container_client, cursor, schema, target_table, log_table):
    # Track if target table needs to be created dynamically
    target_table_exists = table_exists(cursor, schema, target_table)

    for blob in blob_list:
        blob_name = blob.name
        logging.info(f"Processing blob: '{blob_name}'")

        if is_already_ingested(cursor, schema, log_table, blob_name):
            logging.info(f"Skipping '{blob_name}' (already ingested).")
            continue

        try:
            df = download_blob(container_client, blob_name)
        except Exception as e:
            logging.error(f"Failed to download or parse blob '{blob_name}': {e}")
            continue

        if df.empty:
            logging.warning(f"File '{blob_name}' is empty. Skipping.")
            continue

        # Normalize columns to lowercase for consistency with PostgreSQL
        df.columns = df.columns.str.lower()

        # Ensure 'blob_filename' column exists
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s;
        """, (schema, target_table))
        existing_cols = {row[0].lower() for row in cursor.fetchall()}
        if 'blob_filename' not in existing_cols:
            cursor.execute(f'ALTER TABLE {schema}.{target_table} ADD COLUMN blob_filename TEXT;')
            logging.info(f'Added column "blob_filename" to {schema}.{target_table}.')
            cursor.connection.commit()

        # Add the blob_filename column to the DataFrame
        df['blob_filename'] = blob_name

        if not target_table_exists:
            create_table_with_columns(cursor, schema, target_table, df.columns)
            target_table_exists = True
        else:
            # Add any missing columns dynamically except blob_filename which is handled above
            for col in df.columns:
                if col not in existing_cols and col != 'blob_filename':
                    cursor.execute(f'ALTER TABLE {schema}.{target_table} ADD COLUMN {col} TEXT;')
                    logging.info(f'Added missing column "{col}" to {schema}.{target_table}.')
            cursor.connection.commit()

        # Insert data into target table including blob_filename
        logging.info(f"Inserting {len(df)} rows into {schema}.{target_table}...")
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(df.columns)
        insert_query = f"INSERT INTO {schema}.{target_table} ({cols}) VALUES %s"
        try:
            execute_values(cursor, insert_query, tuples)
            cursor.connection.commit()
        except Exception as e:
            logging.error(f"Failed to insert data from '{blob_name}': {e}")
            continue

        # Log ingestion
        log_ingestion(cursor, schema, log_table, blob_name)

        logging.info(f"Finished processing '{blob_name}'.")


def main(): 
    try:
        # Authenticate Azure and get blob client
        blob_service_client = authenticate_azure(ACCOUNT_URL)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        # Connect to Postgres
        logging.info("Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host=PG_HOST,
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT,
        )
        cur = conn.cursor()

        # Initialize DB schema and log tables
        initialize_database(cur, PG_SCHEMA, PG_TABLE, LOG_TABLE)

        # List blobs and ingest
        blobs = list_blobs(container_client, BLOB_PREFIX)
        ingest_blobs(blobs, container_client, cur, PG_SCHEMA, PG_TABLE, LOG_TABLE)

        logging.info("All blobs ingested successfully!")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
