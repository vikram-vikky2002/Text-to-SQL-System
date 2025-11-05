# Text-to-SQL Financial & Operational Analytics PoC

## 1. Overview
This project is a Python 3 proof-of-concept system that answers natural language questions about combined financial (statements, performance metrics) and operational (cargo volumes, port EBIT) data. It:
- Ingests multiple CSVs into a normalized SQLite schema.
- Normalizes metric names (canonical accounts).
- Generates SQL from user questions using heuristics and (optionally) an LLM.
- Executes the SQL safely (read-only).
- Returns a natural language answer (not raw tuples).
- Falls back gracefully if the query is out of scope or unsafe.

Supported capabilities: single metric lookup, multi-year trends, ranking, multi-metric summaries (Revenue, EBITDA, Margin), YOY growth, port rankings, cargo volumes by port, EBIT per MMT, correlation (revenue growth vs margin change), capital employed vs EBIT (ROCE). Optional LLM for more comparative / explanatory questions.

## 2. Setup & Running

### 2.1 Prerequisites
- Python 3.11+ recommended.
- Windows (project tested on Windows; works cross-platform).
- Git (optional).

### 2.2 Installation
```powershell
git clone <repo-url>  # (if under version control)
cd EY
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

### 2.3 Environment Configuration
Create `.env` (optional, auto-loaded if OPENAI_API_KEY not already in environment):
```
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-REPLACE-ME
OPENAI_BASE_URL=https://api.openai.com/v1
LLM_DEBUG=0
```

### 2.4 Build / Rebuild Database
Place CSVs in `dataset/`.
```powershell
python -m src.ingest --db financial.db
```
Generates `financial.db` under project root.

### 2.5 CLI Usage
```powershell
python -m src.main "What was EBITDA in 2024-25?"
python -m src.main "Rank years by Revenue from Operation"
python -m src.main "Top 5 ports by EBIT in 2024-25"
python -m src.main "Compare revenue and EBITDA margin trend for the last 3 years"
python -m src.main "Year over year growth in EBITDA between 2023-24 and 2024-25"
```

### 2.6 API Server
```powershell
python -m uvicorn src.api:app --port 8000
```
Query:
```powershell
Invoke-RestMethod -Uri http://127.0.0.1:8000/ask -Method Post -Body '{"question":"Top 3 ports by EBIT in 2024-25"}' -ContentType 'application/json'
```

### 2.7 Data Quality Profile
```powershell
python -m src.data_quality
```

### 2.8 Tests
```powershell
pytest
```
(Shows pass count, validates ingestion + heuristic + multi-metric outputs.)

## 3. Dependencies
See `requirements.txt` (core items):
- pandas (CSV ingestion, cleaning)
- python-dotenv (optional .env load)
- requests (direct OpenAI API)
- fastapi / uvicorn (API)
- pytest (tests)

LLM libraries from LangChain were removed for simplicity; replaced by raw REST calls.

## 4. Database Schema

### 4.1 Design Rationale
Hybrid star schema: shared dimension tables + narrow fact tables for each domain slice, optimized for clarity in Text-to-SQL rather than minimal storage.

### 4.2 Dimensions
- dim_period(period_id PK, raw_label, start_year, end_year, period_type, quarter_number, sort_key)
- dim_account(account_id PK, raw_name, canonical_name, statement_type, category, sub_category, metric_type)
- dim_port(port_id PK, port_name, domestic_flag, region)
- dim_cargo_type(cargo_type_id PK, name)
- dim_commodity(commodity_id PK, name)
- dim_entity(entity_id PK, name, entity_type)
- dim_fin_type(fin_type_id PK, name)
- dim_roce_category(roce_cat_id PK, name)

### 4.3 Facts
- fact_pnl_annual(account_id FK, period_id FK, value)
- fact_pnl_quarterly(account_id FK, period_id FK, value)
- fact_balance_sheet(account_id FK, period_id FK, value)
- fact_cash_flow(account_id FK, period_id FK, value
)
- fact_roce_external(account_id FK, period_id FK, value)
- fact_roce_internal(port_id FK, account_id FK, period_id FK, value)
- fact_volume(port_id FK, cargo_type_id FK, commodity_id FK, entity_id FK, fin_type_id FK, period_id FK, value)
- fact_roro(port_id FK, period_id FK, value, number_of_cars)

### 4.4 Relationships
- All fact tables link to dim_period via period_id.
- Financial metrics link via dim_account.
- Operational metrics link via port + cargo dimensions.
- ROCE internal joins port and account; external uses account only.
- Views derive ratios (e.g., EBITDA margin) or combined metrics (EBIT per MMT).

### 4.5 Normalization
- Canonical metric names unify variants (EBITDA vs EBIDTA).
- Period textual labels mapped to start/end years for ordering and range queries.

## 5. Design Choices

### 5.1 Architecture
Layers:
1. Ingestion (csv -> clean -> load) with explicit parsing (strip commas, numeric coercion).
2. Schema (SQLite) chosen for simplicity, zero external dependencies.
3. Heuristic NL parser for common patterns (exact match, top N, YOY growth, per-unit ratios).
4. Optional LLM layer only for comparative/analytical phrasing (growth, change, trend, explain).
5. Guard rails: SELECT-only enforcement, table whitelist, canonical name mapping, fallback path.

### 5.2 Prompt Strategy (LLM)
- Single prompt includes compact schema snapshot, canonical metric list, join hints.
- Few-shot examples to constrain style (question -> SQL).
- Post-processing strips surrounding text; first SELECT enforced.
- Reject queries referencing unknown tables or non-SELECT verbs -> fallback.

### 5.3 Heuristic vs LLM Split
- Deterministic heuristics for: single metric, multi-year trend, ranking, top-N ports, volume by port, margin, YOY growth, EBIT per MMT, correlation, capital employed vs EBIT.
- LLM reserved for narrative/explanatory or cross-pattern analytical queries to avoid unnecessary token usage.

### 5.4 Multi-Metric Responses
Special handler returns combined Revenue, EBITDA, Margin lines in one synthesized sentence (reducing multiple queries).

### 5.5 Safety
- No write operations.
- Simple keyword filtering (UPDATE/DELETE/DROP).
- Canonical validation of tables/columns.
- Fallback path ensures the system never returns raw stack traces.

## 6. Limitations / Known Issues
- Advanced analytical queries (e.g., multi-step derived ratios not implemented heuristically) rely on LLM.
- Forecasting is not implemented (placeholder query returns recent trend only).
- Some duplicate metrics may still exist in dim_account (internal consolidation partial).
- No full semantic validation (e.g., EBIT wrongly requested from balance sheet would rely on heuristic corrections).
- Limited correlation (only revenue growth vs margin change).
- No caching of LLM outputs.
- Quarter-level financial operations not yet surfaced in queries (annual focus).
- No authentication layer for API.
- Performance acceptable for PoC; not tuned for very large datasets.

## 7. Example Questions
Heuristic:
- What was EBITDA in 2024-25?
- Rank years by Revenue from Operation.
- Top 3 ports by EBIT in 2024-25.
- List dry cargo volumes by port for 2024-25.
- Year over year growth in EBITDA between 2023-24 and 2024-25.
- Explain change in average capital employed versus EBIT trend.
LLM (if enabled):
- Explain correlation between revenue growth and EBITDA margin change.
- Compare change in ROCE and EBITDA margin trend across the last 4 fiscal years.
- Analyze and forecast EBITDA trend.

## 8. Configuration & Environment Variables
Priority order: explicit OS env vars > .env file.
- LLM_PROVIDER=openai
- OPENAI_API_KEY=sk-...
- OPENAI_BASE_URL (optional; defaults to OpenAI public)
- LLM_DEBUG=1 (prints raw generated SQL)
Unset LLM_PROVIDER or API key -> system runs pure heuristic.

## 9. Error Handling / Graceful Failure
- Out-of-scope queries (e.g., “stock price”) return scoped failure message.
- Empty result sets produce “No matching data” (LLM path) or fallback.
- Invalid periods trigger clarification request (future enhancement).

## 10. Testing Strategy
Pytest coverage (representative):
- Ingestion & row count validity.
- Single metric retrieval.
- Multi-metric combined formatting.
- Port ranking distinctness.
- YOY growth percent computation.
- Cargo volume by port (cargo type filter).
- Capital employed vs EBIT with ROCE derivation.
- Correlation calculation.

## 11. Future Improvements
- Add CAGR, rolling averages, more derived views.
- Expand port-level EBITDA mapping (if granular data available).
- Embedding-based retrieval for robust synonym resolution.
- Add `/metrics` structured JSON endpoint.
- Introduce model confidence scoring.
- Implement SQL plan validation (EXPLAIN integration).
- Enhance semantic validation (metric-to-table enforcement).

## 12. Folder Structure (Key)
```
src/
  ingest.py        (CSV -> SQLite loader)
  nl2sql.py        (heuristic parser)
  engine.py        (orchestrator, answer formatting)
  llm_strategy.py  (LLM prompt & call)
  api.py           (FastAPI app)
  data_quality.py  (profiling script)
tests/             (pytest cases)
dataset/           (input CSVs)
financial.db       (generated database)
```

