import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def run_analysis(db_path="healthcare_costs.db"):
    conn = sqlite3.connect(db_path)
    
    # --- SQL ANALYSIS Q1: Pricing Spread ---
    # Identifying procedures with the widest range in gross pricing
    q1_sql = """
    SELECT p.description, 
           MAX(c.gross_charge) - MIN(c.gross_charge) as price_spread
    FROM charges c
    JOIN procedures p ON c.procedure_id = p.procedure_id
    GROUP BY p.description
    ORDER BY price_spread DESC
    LIMIT 10;
    """
    print(f'raw_sql for question 1:\n {q1_sql}')
    print("\n--- Top 10 Procedures with highest Gross Charge spread ---")
    print(pd.read_sql(q1_sql, conn))

    # --- SQL ANALYSIS Q2: Cash Savings Analysis ---
    # Assessing the gap between Gross and Discounted Cash pricing
    q2_sql = """
    SELECT 
        p.description,
        AVG(c.gross_charge) as avg_gross,
        AVG(c.discounted_cash_charge) as avg_discounted,
        AVG(c.gross_charge - c.discounted_cash_charge) as avg_cash_savings
    FROM charges c
    JOIN procedures p ON c.procedure_id = p.procedure_id
    WHERE c.gross_charge IS NOT NULL AND c.discounted_cash_charge IS NOT NULL
    GROUP BY p.description
    ORDER BY avg_cash_savings DESC
    LIMIT 10;
    """
    print(f'raw_sql for question 2:\n {q2_sql}')
    print("\n--- Top 10 Procedures with highest average Cash Savings ---")
    print(pd.read_sql(q2_sql, conn))

    # --- PYTHON/MATPLOTLIB ANALYSIS Q3: Distribution ---
    q3_sql = """
    SELECT gross_charge FROM charges WHERE gross_charge > 0 AND gross_charge < 50000
    """
    print(f'raw_sql for question 3:\n {q3_sql}')
    # Visualizing the distribution of Gross Charges
    df = pd.read_sql(q3_sql, conn)

    plt.figure(figsize=(10, 6))
    plt.hist(df['gross_charge'], bins=50, color='skyblue', edgecolor='black')
    plt.title("Distribution of Hospital Gross Charges (Under $50k)")
    plt.xlabel("Gross Charge ($)")
    plt.ylabel("Frequency")
    plt.grid(axis='y', alpha=0.5)
    plt.savefig("gross_charge_distribution.png")
    print("\nAnalysis visualization saved as gross_charge_distribution.png")
    
    conn.close()

if __name__ == "__main__":
    run_analysis()