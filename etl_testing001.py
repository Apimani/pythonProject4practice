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

# Output file path
output_file = "ETL_Test_Results.csv"

# List to store query results
query_results = []

# Function to Execute Query and Store Results
def execute_query(query, description):
    try:
        logger.info(f"Executing query: {description}")
        df = pd.read_sql(query, conn)  # Use connection object

        if not df.empty:
            df = df.reset_index(drop=True)  # Reset index to avoid conflicts
            df["Query_Description"] = description  # Add a column for tracking

            # Ensure unique column names
            df.columns = [f"{col}_{i}" if df.columns.duplicated()[i] else col for i, col in enumerate(df.columns)]

            # Reorder columns to maintain structure
            cols = ["Query_Description"] + [col for col in df.columns if col != "Query_Description"]
            df = df[cols]

            query_results.append(df)  # Store in list
            logger.info(f"Query '{description}' executed successfully.")
        else:
            logger.warning(f"No data returned for: {description}")

        print(f"\n{description}:\n", df)
    except Exception as e:
        logger.error(f"Error executing query '{description}': {e}")

# 1. Data Validation Queries
execute_query("SELECT COUNT(*) AS Row_Count FROM large_dataset_src;", "Row Count - Source Table")
execute_query("SELECT COUNT(*) AS Row_Count FROM large_dataset_tgt005;", "Row Count - Target Table")

execute_query(
    "SELECT ID, COUNT(*) FROM large_dataset_tgt005 GROUP BY ID HAVING COUNT(*) > 1;",
    "Duplicate Check"
)

execute_query(
    """SELECT a.* FROM large_dataset_src a
       LEFT JOIN large_dataset_tgt005 b ON a.ID = b.ID
       WHERE b.ID IS NULL;""",
    "Data Comparison Between Source & Target"
)

# 2. Performance and Integrity Checks
execute_query("SELECT * FROM large_dataset_tgt005 WHERE Salary IS NULL;", "Check for NULL Values")

execute_query(
    """SELECT ID, Name, 
       CASE WHEN Name REGEXP '^[0-9]+$' THEN 'Invalid' ELSE 'Valid' END AS data_status 
       FROM large_dataset_tgt005;""",
    "Data Type Validation"
)

execute_query(
    "SELECT ID, COUNT(*) FROM large_dataset_tgt005 GROUP BY ID HAVING COUNT(*) > 1;",
    "Primary Key Uniqueness"
)

# 3. Data Aggregation and Summarization
execute_query(
    "SELECT SUM(Salary) AS Total_Salary, AVG(Salary) AS Avg_Salary, MAX(Salary) AS Max_Salary, MIN(Salary) AS Min_Salary FROM large_dataset_tgt005;",
    "Aggregate Functions"
)

execute_query("SELECT Name, COUNT(*) FROM large_dataset_tgt005 GROUP BY Name;", "Group Data")

# 4. Data Sampling and Extracting Subsets
execute_query("SELECT * FROM large_dataset_tgt005 LIMIT 10;", "Retrieve Top 10 Records")

execute_query("SELECT ID, Name, Department, Joining_Date FROM large_dataset_tgt005;", "Fetch Specific Columns")

# 5. Joins for Data Comparison
execute_query(
    """SELECT a.Performance_Score, b.Performance_Score 
       FROM large_dataset_src a 
       INNER JOIN large_dataset_tgt005 b 
       ON a.ID = b.ID;""",
    "Inner Join"
)

execute_query(
    """SELECT a.Joining_Date, b.Joining_Date 
       FROM large_dataset_src a 
       LEFT JOIN large_dataset_tgt005 b 
       ON a.ID = b.ID;""",
    "Left Join"
)

execute_query(
    """SELECT ID FROM large_dataset_src 
       WHERE ID NOT IN (SELECT ID FROM large_dataset_tgt005)
       UNION
       SELECT ID FROM large_dataset_tgt005 
       WHERE ID NOT IN (SELECT ID FROM large_dataset_src);""",
    "Full Outer Join (Using UNION)"
)

# 6. ETL-Specific Validations
execute_query(
    "SELECT COUNT(*) AS Load_Count FROM large_dataset_tgt005 WHERE DATE(load_date) = CURDATE();",
    "ETL Data Loading Validation"
)

execute_query("SELECT SUM(Performance_Score) AS Source_Sum FROM large_dataset_src;", "Source Sum")
execute_query("SELECT SUM(Performance_Score) AS Target_Sum FROM large_dataset_tgt005;", "Target Sum")

execute_query(
    """SELECT ID FROM large_dataset_src 
       WHERE ID NOT IN (SELECT ID FROM large_dataset_tgt005);""",
    "Check for Missing Records"
)

# 7. Metadata Queries
execute_query("SHOW COLUMNS FROM large_dataset_tgt005;", "View Table Structure")

# Combine all results into one DataFrame and save to CSV
if query_results:
    try:
        final_df = pd.concat(query_results, ignore_index=True)

        # Ensure 'Query_Description' is the first column
        cols = ["Query_Description"] + [col for col in final_df.columns if col != "Query_Description"]
        final_df = final_df[cols]

        # Replace NaN values with blank for better readability
        final_df = final_df.fillna("")

        # Save to CSV
        final_df.to_csv(output_file, index=False)
        logger.info(f"All query results saved in {output_file}")
    except Exception as e:
        logger.error(f"Error while merging DataFrames: {e}")
else:
    logger.warning("No data collected. CSV file not created.")

