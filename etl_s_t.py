import pandas as pd
from sqlalchemy import create_engine
import pymysql  # Ensure pymysql is installed
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Logging configuration
logging.basicConfig(
    filename='etlprocess.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# MySQL engine configuration
try:
    mysql_engine = create_engine('mysql+pymysql://root:Tiger@localhost:3306/mpmani')
    logger.info("Successfully connected to MySQL database.")
except Exception as e:
    logger.error(f"Database connection error: {e}")
    sys.exit(1)  # Exit if DB connection fails

# Step 1: Extract Data
def extract_data():
    try:
        logger.info("Extracting data from CSV file...")
        data = pd.read_csv('large_dataset005.csv')  # Replace with your actual file path
        logger.info(f"Extracted {len(data)} rows.")
        return data
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise

# Step 2: Transform Data (Optional)
def transform_data(data):
    try:
        logger.info("Transforming data...")
        # Add transformation logic if needed
        return data
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

# Step 3: Load Data
def load_data(data, engine, table_name):
    try:
        logger.info("Loading data into MySQL database...")
        data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f"Data successfully loaded into table '{table_name}'.")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

# Main ETL Process
if __name__ == "__main__":
    input_csv = "large_dataset005.csv"
    target_table = "large_dataset_tgt005"

    try:
        extracted_data = extract_data()
        transformed_data = transform_data(extracted_data)  # Uncomment if needed
        load_data(transformed_data, mysql_engine, target_table)
        logger.info("ETL process completed successfully!")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
