import pandas as pd
import sqlite3
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(filename="pipeline.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def load_data(file_path: str) -> pd.DataFrame:
    """Läs in data från CSV eller JSON-fil."""
    try:
        if file_path.endswith('.csv'):
            data = pd.read_csv(file_path)
        elif file_path.endswith('.json'):
            data = pd.read_json(file_path)
        else:
            raise ValueError("Unsupported file format")
        logging.info("Data loaded successfully from %s", file_path)
        return data
    except Exception as e:
        logging.error("Error loading data: %s", e)
        raise

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Bearbeta data."""
    try:
        # Exempel: Konvertera en kolumn till datetime-format
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        logging.info("Data processed successfully")
        return df
    except Exception as e:
        logging.error("Error processing data: %s", e)
        raise

def update_database(df: pd.DataFrame, db_file: str, table_name: str):
    """Uppdatera SQLite-databas med ny data."""
    try:
        with sqlite3.connect(db_file) as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
        logging.info("Database updated successfully")
    except Exception as e:
        logging.error("Error updating database: %s", e)
        raise

if __name__ == "__main__":
    try:
        # Konfigurera filvägar och parametrar
        data_file = "data.csv"
        database_file = "data_pipeline.db"
        table_name = "processed_data"

        # Exekvera flödet
        data = load_data(data_file)
        processed_data = process_data(data)
        update_database(processed_data, database_file, table_name)
    except Exception as e:
        logging.error("Pipeline execution failed: %s", e)
