import pandas as pd
from sqlalchemy import create_engine
import pymysql  # Ensure pymysql is installed
import logging

# Logging Configuration
logging.basicConfig(
    filename="etl_testing.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Database Connection Configuration
try:
    engine = create_engine("mysql+pymysql://root:Tiger@localhost:3306/mpmani")
    conn = engine.connect()
    logger.info("Connected to MySQL database successfully.")
except Exception as e:
    logger.error(f"Database connection error: {e}")
    exit(1)

# Function to Execute Query
def execute_query(query, description):
    try:
        logger.info(f"Executing: {description}")
        df = pd.read_sql(query, conn)
        logger.info(f"Query executed successfully: {description}")
        print(f"\n{description}:\n", df)
        return df
    except Exception as e:
        logger.error(f"Error executing query {description}: {e}")
        return None

# 1. Data Validation Queries
execute_query("SELECT COUNT(*) FROM large_dataset_src;", "Row Count - Source Table")
execute_query("SELECT COUNT(*) FROM large_dataset_tgt005;", "Row Count - Target Table")

execute_query(
    "SELECT ID, COUNT(*) FROM large_dataset_tgt005 GROUP BY ID HAVING COUNT(*) > 1;",
    "Duplicate Check",
)

execute_query(
    """SELECT a.* FROM large_dataset_src a
       LEFT JOIN large_dataset_tgt005 b ON a.ID = b.ID
       WHERE b.ID IS NULL;""",
    "Data Comparison Between Source & Target",
)

# 2. Performance and Integrity Checks
execute_query(
    "SELECT * FROM large_dataset_tgt005 WHERE Salary IS NULL;", "Check for NULL Values"
)

execute_query(
    """SELECT ID, Name, 
       CASE WHEN Name REGEXP '^[0-9]+$' THEN 'Invalid' ELSE 'Valid' END AS data_status 
       FROM large_dataset_tgt005;""",
    "Data Type Validation",
)

execute_query(
    "SELECT ID, COUNT(*) FROM large_dataset_tgt005 GROUP BY ID HAVING COUNT(*) > 1;",
    "Primary Key Uniqueness",
)

# 3. Data Aggregation and Summarization
execute_query(
    "SELECT SUM(Salary), AVG(Salary), MAX(Salary), MIN(Salary) FROM large_dataset_tgt005;",
    "Aggregate Functions",
)

execute_query(
    "SELECT Name, COUNT(*) FROM large_dataset_tgt005 GROUP BY Name;", "Group Data"
)

# 4. Data Sampling and Extracting Subsets
execute_query("SELECT * FROM large_dataset_tgt005 LIMIT 10;", "Retrieve Top 10 Records")

execute_query("SELECT ID, Name, Department, Joining_Date FROM large_dataset_tgt005;", "Fetch Specific Columns")

# 5. Joins for Data Comparison
execute_query(
    """SELECT a.Performance_Score, b.Performance_Score 
       FROM large_dataset_src a 
       INNER JOIN large_dataset_tgt005 b 
       ON a.ID = b.ID;""",
    "Inner Join",
)

execute_query(
    """SELECT a.Joining_Date, b.Joining_Date 
       FROM large_dataset_src a 
       LEFT JOIN large_dataset_tgt005 b 
       ON a.ID = b.ID;""",
    "Left Join",
)

execute_query(
    """SELECT ID FROM large_dataset_src 
       WHERE ID NOT IN (SELECT ID FROM large_dataset_tgt005)
       UNION
       SELECT ID FROM large_dataset_tgt 
       WHERE ID NOT IN (SELECT ID FROM large_dataset_src);""",
    "Full Outer Join (Using UNION)",
)

# 6. Data Cleaning
execute_query(
    """DELETE t1 FROM large_dataset_tgt005 t1
       INNER JOIN (
           SELECT ID, MIN(Joining_Date) AS min_date FROM large_dataset_tgt005 
           GROUP BY ID
       ) t2 ON t1.ID = t2.ID AND t1.Joining_Date > t2.min_date;""",
    "Remove Duplicates",
)

execute_query(
    "UPDATE large_dataset_tgt005 SET ID = 5 WHERE ID = 1111111;", "Update Data"
)

# 7. ETL-Specific Validations
execute_query(
    "SELECT COUNT(*) FROM large_dataset_tgt005 WHERE DATE(load_date) = CURDATE();",
    "ETL Data Loading Validation",
)

execute_query("SELECT SUM(Performance_Score) AS source_sum FROM large_dataset_src;", "Source Sum")

execute_query("SELECT SUM(Performance_Score) AS target_sum FROM large_dataset_tgt005;", "Target Sum")

execute_query(
    """SELECT ID FROM large_dataset_src 
       WHERE ID NOT IN (SELECT ID FROM large_dataset_tgt005);""",
    "Check for Missing Records",
)

# 8. Metadata Queries
execute_query("SHOW COLUMNS FROM large_dataset_tgt005;", "View Table Structure")

# Close Connection
conn.close()
logger.info("Database connection closed.")
