import sqlite3
import os

DB_PATH = "healthcare_costs.db"

def initialize_database():
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