import argparse
import sqlite3
from pathlib import Path
import re
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = ROOT / 'dataset'

NUMERIC_CLEAN_RE = re.compile(r'[,\"]')

def parse_period_label(label: str):
    """Parse fiscal period label like '2024-25' into (start_year, end_year)."""
    if not isinstance(label, str):
        return None, None
    m = re.match(r'(20\d{2})-(\d{2})', label)
    if m:
        start = int(m.group(1))
        end_suffix = int(m.group(2))
        end = start // 100 * 100 + end_suffix  # assumes same century
        if end < start:  # rollover safeguard
            end += 100
        return start, end
    # Could be simple year e.g. 2024
    m2 = re.match(r'(20\d{2})', label)
    if m2:
        year = int(m2.group(1))
        return year, year
    return None, None

def clean_number(val):
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if s == '':
        return None
    # Remove commas and quotes
    s = NUMERIC_CLEAN_RE.sub('', s)
    try:
        return float(s)
    except ValueError:
        return None

def ensure_periods(periods, conn):
    cur = conn.cursor()
    for label in sorted(set(periods)):
        start, end = parse_period_label(label)
        sort_key = start if start else None
        cur.execute("INSERT OR IGNORE INTO dim_period(raw_label,start_year,end_year,sort_key) VALUES (?,?,?,?)",
                    (label, start, end, sort_key))
    conn.commit()

def get_period_id(conn, label):
    cur = conn.cursor()
    cur.execute("SELECT period_id FROM dim_period WHERE raw_label=?", (label,))
    row = cur.fetchone()
    return row[0] if row else None

def upsert_account(conn, name, canonical_name, statement_type, category=None, sub_category=None, sub_sub_category=None, metric_type='absolute'):
    cur = conn.cursor()
    cur.execute("SELECT account_id FROM dim_account WHERE name=? AND statement_type=?", (name, statement_type))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO dim_account(name, canonical_name, statement_type, category, sub_category, sub_sub_category, metric_type) VALUES (?,?,?,?,?,?,?)",
                (name, canonical_name, statement_type, category, sub_category, sub_sub_category, metric_type))
    conn.commit()
    return cur.lastrowid

def load_synonyms():
    dd_file = ROOT / 'src' / 'data_dictionary.yaml'
    with open(dd_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def canonicalize(name: str, synonyms_map) -> str:
    if not isinstance(name, str):
        return name
    lowered = name.lower().strip()
    for canonical, syns in synonyms_map.get('synonyms', {}).items():
        for s in syns:
            if lowered == s.lower():
                return canonical
    return name.strip()

def ingest_balance_sheet(conn, synonyms):
    path = DATASET_DIR / 'BalanceSheet.csv'
    if not path.exists():
        return
    df = pd.read_csv(path)
    ensure_periods(df['Period'], conn)
    for _, row in df.iterrows():
        acct_name = str(row['Line Item']).strip()
        canonical = canonicalize(acct_name, synonyms)
        acct_id = upsert_account(conn, acct_name, canonical, 'BalanceSheet', row.get('Category'), row.get('SubCategory'), row.get('SubSubCategory'))
        pid = get_period_id(conn, row['Period'])
        val = clean_number(row['Value'])
        conn.execute("INSERT INTO fact_balance_sheet(account_id, period_id, value) VALUES (?,?,?)", (acct_id, pid, val))
    conn.commit()

def ingest_cash_flow(conn, synonyms):
    path = DATASET_DIR / 'CashFlowStatement.csv'
    if not path.exists():
        return
    df = pd.read_csv(path)
    ensure_periods(df['Period'], conn)
    for _, row in df.iterrows():
        acct_name = str(row['Item']).strip()
        canonical = canonicalize(acct_name, synonyms)
        acct_id = upsert_account(conn, acct_name, canonical, 'CashFlow', row.get('Category'))
        pid = get_period_id(conn, row['Period'])
        val = clean_number(row['Value'])
        conn.execute("INSERT INTO fact_cash_flow(account_id, period_id, value) VALUES (?,?,?)", (acct_id, pid, val))
    conn.commit()

def ingest_consolidated_pnl(conn, synonyms):
    path = DATASET_DIR / 'Consolidated PnL.csv'
    if not path.exists():
        return
    df = pd.read_csv(path)
    ensure_periods(df['Period'], conn)
    for _, row in df.iterrows():
        acct_name = str(row['Line Item']).strip()
        canonical = canonicalize(acct_name, synonyms)
        metric_type = 'ratio' if '%' in acct_name or canonical.endswith('%') or canonical.lower().endswith('cagr') else 'absolute'
        acct_id = upsert_account(conn, acct_name, canonical, 'PnLAnnual', metric_type=metric_type)
        pid = get_period_id(conn, row['Period'])
        val = clean_number(row['Value'])
        conn.execute("INSERT INTO fact_pnl_annual(account_id, period_id, value) VALUES (?,?,?)", (acct_id, pid, val))
    conn.commit()

def ingest_quarterly_pnl(conn, synonyms):
    path = DATASET_DIR / 'Quarterly PnL.csv'
    if not path.exists():
        return
    df = pd.read_csv(path)
    ensure_periods(df['Period'], conn)
    for _, row in df.iterrows():
        acct_name = str(row['Item']).strip()
        canonical = canonicalize(acct_name, synonyms)
        acct_id = upsert_account(conn, acct_name, canonical, 'PnLQuarterly', row.get('Category'))
        pid = get_period_id(conn, row['Period'])
        val = clean_number(row['Value'])
        conn.execute("INSERT INTO fact_pnl_quarterly(account_id, period_id, value) VALUES (?,?,?)", (acct_id, pid, val))
    conn.commit()

def ingest_roce_external(conn, synonyms):
    path = DATASET_DIR / 'ROCE External.csv'
    if not path.exists():
        return
    df = pd.read_csv(path)
    ensure_periods(df['Period'], conn)
    for _, row in df.iterrows():
        acct_name = str(row['Particular']).strip()
        canonical = canonicalize(acct_name, synonyms)
        acct_id = upsert_account(conn, acct_name, canonical, 'ROCEExternal')
        pid = get_period_id(conn, row['Period'])
        val = clean_number(row['Value'])
        conn.execute("INSERT INTO fact_roce_external(account_id, period_id, value) VALUES (?,?,?)", (acct_id, pid, val))
    conn.commit()

def ingest_roce_internal(conn, synonyms):
    path = DATASET_DIR / 'ROCE Internal.csv'
    if not path.exists():
        return
    df = pd.read_csv(path)
    ensure_periods(df['Period'], conn)
    # Build ROCE category dim
    cur = conn.cursor()
    for cat in sorted(set(df['Category'])):
        cur.execute("INSERT OR IGNORE INTO dim_roce_category(name) VALUES (?)", (cat,))
    conn.commit()
    # Ports
    for port in sorted(set(df['Port'])):
        cur.execute("INSERT OR IGNORE INTO dim_port(port_name) VALUES (?)", (port,))
    conn.commit()
    # Map ids
    cat_map = {r[1]: r[0] for r in cur.execute("SELECT roce_category_id,name FROM dim_roce_category")}
    port_map = {r[1]: r[0] for r in cur.execute("SELECT port_id,port_name FROM dim_port")}
    for _, row in df.iterrows():
        acct_name = str(row['Line Item']).strip()
        canonical = canonicalize(acct_name, synonyms)
        acct_id = upsert_account(conn, acct_name, canonical, 'ROCEInternal')
        pid = get_period_id(conn, row['Period'])
        val = clean_number(row['Value'])
        conn.execute("INSERT INTO fact_roce_internal(roce_category_id, port_id, account_id, period_id, value) VALUES (?,?,?,?,?)",
                     (cat_map.get(row['Category']), port_map.get(row['Port']), acct_id, pid, val))
    conn.commit()

def ingest_roro(conn):
    path = DATASET_DIR / 'RORO.csv'
    if not path.exists():
        return
    df = pd.read_csv(path)
    ensure_periods(df['Period'], conn)
    cur = conn.cursor()
    for port in sorted(set(df['Port'])):
        cur.execute("INSERT OR IGNORE INTO dim_port(port_name) VALUES (?)", (port,))
    conn.commit()
    port_map = {r[1]: r[0] for r in cur.execute("SELECT port_id,port_name FROM dim_port")}
    for _, row in df.iterrows():
        pid = get_period_id(conn, row['Period'])
        port_id = port_map.get(row['Port'])
        val = clean_number(row['Value'])
        cars = clean_number(row.get('Number of Cars'))
        conn.execute("INSERT INTO fact_roro(port_id, period_id, type, value, number_of_cars) VALUES (?,?,?,?,?)",
                     (port_id, pid, row.get('Type'), val, cars))
    conn.commit()

def ingest_volumes(conn):
    path = DATASET_DIR / 'Volumes.csv'
    if not path.exists():
        return
    df = pd.read_csv(path)
    ensure_periods(df['Period'], conn)
    cur = conn.cursor()
    def distinct(series):
        vals = []
        for v in series.unique():
            if pd.isna(v):
                continue
            s = str(v).strip()
            if s == '' or s.lower() in {'nan', 'none'}:
                continue
            vals.append(s)
        return sorted(set(vals))

    for port in distinct(df['Port']):
        cur.execute("INSERT OR IGNORE INTO dim_port(port_name) VALUES (?)", (port,))
    for ct in distinct(df['State']):  # 'State' actually cargo type
        cur.execute("INSERT OR IGNORE INTO dim_cargo_type(name) VALUES (?)", (ct,))
    for com in distinct(df['Commodity']):
        cur.execute("INSERT OR IGNORE INTO dim_commodity(name) VALUES (?)", (com,))
    for ent in distinct(df['Entity']):
        cur.execute("INSERT OR IGNORE INTO dim_entity(name) VALUES (?)", (ent,))
    for fin in distinct(df['Type']):
        cur.execute("INSERT OR IGNORE INTO dim_fin_type(name) VALUES (?)", (fin,))
    conn.commit()
    # Maps
    port_map = {r[1]: r[0] for r in cur.execute("SELECT port_id,port_name FROM dim_port")}
    cargo_map = {r[1]: r[0] for r in cur.execute("SELECT cargo_type_id,name FROM dim_cargo_type")}
    com_map = {r[1]: r[0] for r in cur.execute("SELECT commodity_id,name FROM dim_commodity")}
    ent_map = {r[1]: r[0] for r in cur.execute("SELECT entity_id,name FROM dim_entity")}
    fin_map = {r[1]: r[0] for r in cur.execute("SELECT fin_type_id,name FROM dim_fin_type")}
    for _, row in df.iterrows():
        pid = get_period_id(conn, row['Period'])
        conn.execute("INSERT INTO fact_volume(port_id,cargo_type_id,commodity_id,entity_id,fin_type_id,period_id,volume_value) VALUES (?,?,?,?,?,?,?)",
                     (port_map.get(row['Port']), cargo_map.get(row['State']), com_map.get(row['Commodity']), ent_map.get(row['Entity']), fin_map.get(row['Type']), pid, clean_number(row['Value'])))
    conn.commit()

def initialize_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    ddl = (ROOT / 'src' / 'schema.sql').read_text(encoding='utf-8')
    conn.executescript(ddl)
    return conn

def build(db_path: Path):
    synonyms = load_synonyms()
    conn = initialize_db(db_path)
    ingest_balance_sheet(conn, synonyms)
    ingest_cash_flow(conn, synonyms)
    ingest_consolidated_pnl(conn, synonyms)
    ingest_quarterly_pnl(conn, synonyms)
    ingest_roce_external(conn, synonyms)
    ingest_roce_internal(conn, synonyms)
    ingest_roro(conn)
    ingest_volumes(conn)
    conn.close()

def main():
    parser = argparse.ArgumentParser(description='Ingest CSV data into SQLite DB.')
    parser.add_argument('--db', default='financial.db', help='SQLite database file name')
    args = parser.parse_args()
    db_path = ROOT / args.db
    build(db_path)
    print(f"Database built at {db_path}")

if __name__ == '__main__':
    main()
