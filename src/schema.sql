-- Schema DDL for Text-to-SQL PoC
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dim_period (
  period_id INTEGER PRIMARY KEY AUTOINCREMENT,
  raw_label TEXT UNIQUE NOT NULL,
  start_year INTEGER,
  end_year INTEGER,
  period_type TEXT DEFAULT 'FY',
  quarter INTEGER,
  sort_key INTEGER
);

CREATE TABLE IF NOT EXISTS dim_account (
  account_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  canonical_name TEXT NOT NULL,
  statement_type TEXT NOT NULL,
  category TEXT,
  sub_category TEXT,
  sub_sub_category TEXT,
  metric_type TEXT DEFAULT 'absolute'
);

CREATE TABLE IF NOT EXISTS dim_port (
  port_id INTEGER PRIMARY KEY AUTOINCREMENT,
  port_name TEXT UNIQUE NOT NULL,
  domestic_flag INTEGER DEFAULT 1,
  region TEXT
);

CREATE TABLE IF NOT EXISTS dim_cargo_type (
  cargo_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_commodity (
  commodity_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_entity (
  entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL,
  entity_type TEXT
);

CREATE TABLE IF NOT EXISTS dim_fin_type (
  fin_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_roce_category (
  roce_category_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);

-- Fact tables
CREATE TABLE IF NOT EXISTS fact_balance_sheet (
  account_id INTEGER NOT NULL,
  period_id INTEGER NOT NULL,
  value REAL,
  FOREIGN KEY(account_id) REFERENCES dim_account(account_id),
  FOREIGN KEY(period_id) REFERENCES dim_period(period_id)
);

CREATE TABLE IF NOT EXISTS fact_cash_flow (
  account_id INTEGER NOT NULL,
  period_id INTEGER NOT NULL,
  value REAL,
  FOREIGN KEY(account_id) REFERENCES dim_account(account_id),
  FOREIGN KEY(period_id) REFERENCES dim_period(period_id)
);

CREATE TABLE IF NOT EXISTS fact_pnl_annual (
  account_id INTEGER NOT NULL,
  period_id INTEGER NOT NULL,
  value REAL,
  FOREIGN KEY(account_id) REFERENCES dim_account(account_id),
  FOREIGN KEY(period_id) REFERENCES dim_period(period_id)
);

CREATE TABLE IF NOT EXISTS fact_pnl_quarterly (
  account_id INTEGER NOT NULL,
  period_id INTEGER NOT NULL,
  value REAL,
  FOREIGN KEY(account_id) REFERENCES dim_account(account_id),
  FOREIGN KEY(period_id) REFERENCES dim_period(period_id)
);

CREATE TABLE IF NOT EXISTS fact_roce_external (
  account_id INTEGER NOT NULL,
  period_id INTEGER NOT NULL,
  value REAL,
  FOREIGN KEY(account_id) REFERENCES dim_account(account_id),
  FOREIGN KEY(period_id) REFERENCES dim_period(period_id)
);

CREATE TABLE IF NOT EXISTS fact_roce_internal (
  roce_category_id INTEGER,
  port_id INTEGER,
  account_id INTEGER NOT NULL,
  period_id INTEGER NOT NULL,
  value REAL,
  FOREIGN KEY(roce_category_id) REFERENCES dim_roce_category(roce_category_id),
  FOREIGN KEY(port_id) REFERENCES dim_port(port_id),
  FOREIGN KEY(account_id) REFERENCES dim_account(account_id),
  FOREIGN KEY(period_id) REFERENCES dim_period(period_id)
);

CREATE TABLE IF NOT EXISTS fact_roro (
  port_id INTEGER,
  period_id INTEGER,
  type TEXT,
  value REAL,
  number_of_cars INTEGER,
  FOREIGN KEY(port_id) REFERENCES dim_port(port_id),
  FOREIGN KEY(period_id) REFERENCES dim_period(period_id)
);

CREATE TABLE IF NOT EXISTS fact_volume (
  port_id INTEGER,
  cargo_type_id INTEGER,
  commodity_id INTEGER,
  entity_id INTEGER,
  fin_type_id INTEGER,
  period_id INTEGER,
  volume_value REAL,
  FOREIGN KEY(port_id) REFERENCES dim_port(port_id),
  FOREIGN KEY(cargo_type_id) REFERENCES dim_cargo_type(cargo_type_id),
  FOREIGN KEY(commodity_id) REFERENCES dim_commodity(commodity_id),
  FOREIGN KEY(entity_id) REFERENCES dim_entity(entity_id),
  FOREIGN KEY(fin_type_id) REFERENCES dim_fin_type(fin_type_id),
  FOREIGN KEY(period_id) REFERENCES dim_period(period_id)
);

-- Derived Views
CREATE VIEW IF NOT EXISTS view_ebitda_margin AS
SELECT p.raw_label AS period,
       ebit.value AS ebitda,
       rev.value AS revenue,
       CASE WHEN rev.value != 0 THEN ebit.value / rev.value ELSE NULL END AS ebitda_margin
FROM fact_pnl_annual ebit
JOIN dim_account a1 ON ebit.account_id = a1.account_id AND a1.canonical_name = 'EBITDA'
JOIN fact_pnl_annual rev ON rev.period_id = ebit.period_id
JOIN dim_account a2 ON rev.account_id = a2.account_id AND a2.canonical_name = 'Revenue from Operation'
JOIN dim_period p ON p.period_id = ebit.period_id;

CREATE VIEW IF NOT EXISTS view_roce AS
SELECT p.raw_label AS period,
       ebit.value AS ebit,
       cap.value AS average_capital_employed,
       CASE WHEN cap.value != 0 THEN ebit.value / cap.value ELSE NULL END AS roce
FROM fact_roce_external ebit
JOIN dim_account a1 ON ebit.account_id = a1.account_id AND a1.canonical_name = 'EBIT'
JOIN fact_roce_external cap ON cap.period_id = ebit.period_id
JOIN dim_account a2 ON cap.account_id = a2.account_id AND a2.canonical_name = 'Average capital employed'
JOIN dim_period p ON p.period_id = ebit.period_id;

-- Port EBIT per cargo volume view (cross-domain)
CREATE VIEW IF NOT EXISTS view_port_ebit_volume AS
SELECT p.raw_label AS period,
       dp.port_name,
       e.value AS ebit,
       COALESCE(SUM(v.volume_value),0) AS total_volume,
       CASE WHEN SUM(v.volume_value) != 0 THEN e.value / SUM(v.volume_value) ELSE NULL END AS ebit_per_mmt
FROM fact_roce_internal e
JOIN dim_account a ON e.account_id=a.account_id AND a.canonical_name='EBIT'
JOIN dim_period p ON p.period_id=e.period_id
JOIN dim_port dp ON dp.port_id=e.port_id
LEFT JOIN fact_volume v ON v.port_id=e.port_id AND v.period_id=e.period_id
GROUP BY p.raw_label, dp.port_name, e.value, p.sort_key
ORDER BY p.sort_key DESC, e.value DESC;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_fact_balance_sheet_period_account ON fact_balance_sheet(period_id, account_id);
CREATE INDEX IF NOT EXISTS idx_fact_cash_flow_period_account ON fact_cash_flow(period_id, account_id);
CREATE INDEX IF NOT EXISTS idx_fact_pnl_annual_period_account ON fact_pnl_annual(period_id, account_id);
CREATE INDEX IF NOT EXISTS idx_fact_roce_internal_port_period ON fact_roce_internal(port_id, period_id);
CREATE INDEX IF NOT EXISTS idx_fact_volume_port_period ON fact_volume(port_id, period_id);
