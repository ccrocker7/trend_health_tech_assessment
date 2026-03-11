-- schema.sql

-- Clear existing tables to ensure a clean state during development
DROP TABLE IF EXISTS charges;
DROP TABLE IF EXISTS procedures;

-- Dimension Table: Stores unique procedure descriptions
CREATE TABLE procedures (
    procedure_id INTEGER PRIMARY KEY AUTOINCREMENT,
    description TEXT UNIQUE NOT NULL
);

-- Fact Table: Stores pricing data with a unique constraint to prevent duplicates
CREATE TABLE charges (
    charge_id INTEGER PRIMARY KEY AUTOINCREMENT,
    procedure_id INTEGER NOT NULL,
    payer_name TEXT,
    plan_name TEXT,
    gross_charge REAL,
    discounted_cash_charge REAL,
    negotiated_dollar_charge REAL,
    min_charge REAL,
    max_charge REAL,
    -- A UNIQUE constraint to prevent duplicate entries for the same procedure, payer, and plan combination.     UNIQUE(procedure_id, payer_name, plan_name),
    FOREIGN KEY (procedure_id) REFERENCES procedures(procedure_id)
);

-- Indexes for high-performance query execution
CREATE INDEX idx_proc_desc ON procedures(description);
CREATE INDEX idx_charges_proc_id ON charges(procedure_id);