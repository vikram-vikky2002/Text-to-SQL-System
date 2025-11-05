"""Data quality and profiling utilities for the Text-to-SQL PoC."""
from pathlib import Path
import sqlite3
from typing import Dict, Any, List, Tuple
from .nl2sql import ROOT

DB_PATH = ROOT / 'financial.db'

def table_row_counts(conn) -> Dict[str,int]:
    cur = conn.cursor()
    tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    out = {}
    for (t,) in tables:
        if t.startswith('sqlite_'): continue
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        out[t] = cur.fetchone()[0]
    return out

def missing_value_scan(conn) -> List[Tuple[str,Dict[str,int]]]:
    """Return per-table dict of missing counts for metric columns."""
    cur = conn.cursor()
    mapping = {
        'fact_balance_sheet': ['value'],
        'fact_cash_flow': ['value'],
        'fact_pnl_annual': ['value'],
        'fact_pnl_quarterly': ['value'],
        'fact_roce_external': ['value'],
        'fact_roce_internal': ['value'],
        'fact_roro': ['value','number_of_cars'],
        'fact_volume': ['volume_value']
    }
    results = []
    for table, cols in mapping.items():
        col_missing = {}
        for col in cols:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
            col_missing[col] = cur.fetchone()[0]
        results.append((table, col_missing))
    return results

def duplicate_accounts(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT canonical_name, COUNT(*) FROM dim_account GROUP BY canonical_name HAVING COUNT(*)>1")
    dups = cur.fetchall()
    return len(dups)

def reconciliation_roce(conn) -> Dict[str,Any]:
    """Check EBIT sum of ports vs external EBIT for each period."""
    cur = conn.cursor()
    cur.execute("""
    SELECT p.raw_label,
           (SELECT SUM(fri.value) FROM fact_roce_internal fri JOIN dim_account ai ON fri.account_id=ai.account_id AND ai.canonical_name='EBIT' WHERE fri.period_id=p.period_id) AS internal_ebit_sum,
           (SELECT fe.value FROM fact_roce_external fe JOIN dim_account ae ON fe.account_id=ae.account_id AND ae.canonical_name='EBIT' WHERE fe.period_id=p.period_id) AS external_ebit
    FROM dim_period p ORDER BY p.sort_key DESC
    """)
    rows = cur.fetchall()
    out = {}
    for period, internal_sum, external_val in rows:
        if internal_sum is None or external_val is None:
            continue
        diff = internal_sum - external_val
        pct = diff / external_val if external_val else None
        out[period] = {"internal_sum": internal_sum, "external": external_val, "diff": diff, "pct_diff": pct}
    return out

def run_profile(db_path=DB_PATH):
    if not db_path.exists():
        print("Database not found; run ingestion first.")
        return
    conn = sqlite3.connect(db_path)
    print("Row counts:")
    for t, c in table_row_counts(conn).items():
        print(f"  {t}: {c}")
    print("\nMissing value scan:")
    for t, d in missing_value_scan(conn):
        parts = ", ".join([f"{col}={cnt}" for col, cnt in d.items()])
        print(f"  {t}: {parts}")
    print("\nDuplicate canonical accounts:", duplicate_accounts(conn))
    print("\nROCE reconciliation (internal vs external):")
    for period, vals in reconciliation_roce(conn).items():
        pct = vals['pct_diff']
        pct_str = f"{pct:.2%}" if pct is not None else 'n/a'
        print(f"  {period}: internal={vals['internal_sum']:.2f} external={vals['external']:.2f} diff={vals['diff']:.2f} ({pct_str})")
    conn.close()

if __name__ == '__main__':
    run_profile()