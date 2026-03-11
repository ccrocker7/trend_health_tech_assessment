import sqlite3
import os

DB_PATH = "healthcare_costs.db"

def initialize_database() -> None:
    """
    Initializes the SQLite database schema and verifies table integrity.

    This function executes the SQL statements contained in 'schema.sql' to set up 
    the necessary tables for the ETL pipeline. It includes a pre-flight 
    validation step to confirm that all required tables exist in the database 
    metadata before allowing the pipeline to proceed.

    Raises:
        FileNotFoundError: If the 'schema.sql' file is missing.
        sqlite3.Error: If the database connection or script execution fails.
        RuntimeError: If the validation step confirms that the required 
            tables ('procedures', 'charges') were not created successfully.

    Returns:
        None
    """
    with sqlite3.connect(DB_PATH) as conn:
        # 1. Run the schema creation based on the schema.sql file defined in the last phase
        with open("schema.sql", "r") as f:
            conn.executescript(f.read())
        
        # 2. Pre-flight Validation: verify that the above tables were created successfully
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        required_tables = {"procedures", "charges"}
        if required_tables.issubset(set(tables)):
            print(f"Success! Tables {required_tables} created and verified.")
        else:
            raise RuntimeError(f"Schema initialization failed. Found: {tables}")

if __name__ == "__main__":
    initialize_database()