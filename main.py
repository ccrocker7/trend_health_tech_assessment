# main.py
from ingest import ingest_data
from analyze import run_analysis
from setup_db import initialize_database
import sqlite3

DATA_URL = "https://www.wellstar.org/-/media/project/wellstar/org/documents/august-2025-price-transparency-standard-charges/58-2032904_wellstar-kennestone-hospital_standardcharges.csv"
DB_PATH = "healthcare_costs.db"

if __name__ == "__main__":
    print("--- Starting ETL Pipeline ---")
    initialize_database()
    ingest_data(DATA_URL)
    print("--- Pipeline Ingestion Complete. Running Analysis... ---")
    run_analysis()
    print("--- Execution Finished! Check for 'gross_charge_distribution.png' ---")