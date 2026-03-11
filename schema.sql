-- schema.sql

-- Drop existing tables to ensure a clean state for re-runs
DROP TABLE IF EXISTS charges;
DROP TABLE IF EXISTS procedures;

-- Procedures: The "Dimension" table
CREATE TABLE procedures (
    procedure_id INTEGER PRIMARY KEY AUTOINCREMENT,
    description TEXT UNIQUE NOT NULL
);

-- Charges: The "Fact" table
CREATE TABLE charges (
    charge_id INTEGER PRIMARY KEY AUTOINCREMENT,
    procedure_id INTEGER NOT NULL,
    payer_name TEXT,
    plan_name TEXT,
    gross_charge NUMERIC,
    discounted_cash_charge NUMERIC,
    negotiated_dollar_charge NUMERIC,
    min_charge NUMERIC,
    max_charge NUMERIC,
    FOREIGN KEY (procedure_id) REFERENCES procedures(procedure_id)
);

-- Indexes for performance
CREATE INDEX idx_proc_desc ON procedures(description);
CREATE INDEX idx_payer_name ON charges(payer_name);