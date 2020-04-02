import os
from pathlib import Path
from dotenv import load_dotenv
from psycopg2 import connect
from psycopg2.extras import execute_values


def get_root_folder() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent


def get_connection():
    """Create postgres connection"""
    root_path = get_root_folder()
    load_dotenv(root_path.joinpath('src/database.env').as_posix())
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('POSTGRES_DB')
    host = os.getenv('POSTGRES_HOST')

    conn = connect(
        host=host,
        port=5432,
        dbname=db_name,
        user=user,
        password=password
    )
    return conn


def insert_to_table(table_name, column_names, values):
    """Insert data to database.table"""
    cols = ', '.join(column_names)
    query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
    conn = get_connection()
    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()
    conn.close()
