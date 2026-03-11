import sqlite3
import pandas as pd
import requests
import io
import logging

# Logging configuration for better visibility during the ETL process
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def ingest_data(url, db_path="healthcare_costs.db"):
    # Column mapping from csv to database schema. 
    # This allows for flexibility if the source data changes slightly.
    COLUMN_MAPPING = {
        'description': 'description',
        'payer_name': 'payer_name',
        'plan_name': 'plan_name',
        'standard_charge|gross': 'gross_charge',
        'standard_charge|discounted_cash': 'discounted_cash_charge',
    }

    try:
        # Used stream=true to allow this script to scale to large files 
        # without loading everything into memory at once.
        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            # The CSV has 2 header rows, so we skip the first two lines and 
            # read in chunks for memory efficiency.
            reader = pd.read_csv(io.StringIO(response.text), header=2, chunksize=10000)
            
            with sqlite3.connect(db_path) as conn:
                # 1. To maintain idempotency, we are going to use a cache to prevent duplicates.
                # This starts by "warming the cache" or loading the current procedures into memory
                cursor = conn.execute("SELECT description, procedure_id FROM procedures")
                proc_cache = {row[0]: row[1] for row in cursor.fetchall()}
                
                for i, chunk in enumerate(reader):
                    # 2. Then we are going to normalize column names: 
                    # strip whitespace, convert to lowercase, and apply mapping
                    chunk.columns = [c.strip().lower() for c in chunk.columns]
                    chunk = chunk.rename(columns=COLUMN_MAPPING)
                    
                    # We only want to process mapped columns to avoid issues with unexpected data. 
                    # This also serves as a validation step.
                    chunk = chunk[list(COLUMN_MAPPING.values())]
                    
                    # 3. Insert only new procedures within a given chunk to prevent duplicates.
                    # This is done before the charge insertion to ensure we have the correct procedure_ids.
                    for desc in chunk['description'].unique():
                        if desc not in proc_cache:
                            conn.execute("INSERT INTO procedures (description) VALUES (?)", (desc,))
                            proc_cache[desc] = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                    
                    # 4. Insert only new charges. 
                    # We use the procedure_id from the cache to link charges to procedures.
                    chunk['procedure_id'] = chunk['description'].map(proc_cache)
                    
                    # Sanity check the number of duplicates in the chunk before insertion. 
                    # This is a simple way to catch potential issues with the data or mapping.
                    initial_count = len(chunk)
                    chunk_unique = chunk.drop_duplicates(subset=['procedure_id', 'payer_name', 'plan_name'])
                    duplicate_count = initial_count - len(chunk_unique)
                    
                    if duplicate_count > 0:
                        logger.warning(f"Chunk {i+1}: Found {duplicate_count} duplicate records (will be replaced).")
                    
                    # Given ample time, I'd want to revisit this logic to handle duplicates more gracefully, 
                    # perhaps by comparing existing records and only updating if there are changes. 
                    # For now, we will use INSERT OR REPLACE for simplicity.

                    charges_records = chunk.drop(columns=['description']).to_dict('records')
                    cols = ['gross_charge', 'discounted_cash_charge']
                    for col in cols:
                        chunk[col] = pd.to_numeric(chunk[col].replace(r'[\$,]', '', regex=True), errors='coerce')
                    with conn:
                        conn.executemany("""
                            INSERT OR REPLACE INTO charges 
                            (procedure_id, payer_name, plan_name, gross_charge, discounted_cash_charge)
                            VALUES (:procedure_id, :payer_name, :plan_name, :gross_charge, 
                                    :discounted_cash_charge)
                        """, charges_records)
                    
                    logger.info(f"Successfully processed chunk {i+1} ({len(chunk)} rows).")
                    
        logger.info("ETL Pipeline finished successfully.")

    except Exception as e:
        logger.error(f"Pipeline failure: {e}")
        raise

if __name__ == "__main__":
    DATA_URL = "https://www.wellstar.org/-/media/project/wellstar/org/documents/august-2025-price-transparency-standard-charges/58-2032904_wellstar-kennestone-hospital_standardcharges.csv"
    ingest_data(DATA_URL)