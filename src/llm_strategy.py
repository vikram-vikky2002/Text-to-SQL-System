import os
from typing import Optional, Tuple
from pathlib import Path
import sqlite3
import re
import json
import requests

ROOT = Path(__file__).resolve().parent.parent

# Allow SELECT and WITH (CTE) statements
SAFE_SQL_REGEX = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
FORBIDDEN_REGEX = re.compile(r"\b(UPDATE|DELETE|INSERT|DROP|ALTER|CREATE\s+TABLE)\b", re.IGNORECASE)

EXAMPLES = [
    ("What was EBITDA in 2024-25?",
     "SELECT p.raw_label AS period, f.value AS ebitda FROM fact_pnl_annual f JOIN dim_account a ON f.account_id=a.account_id JOIN dim_period p ON p.period_id=f.period_id WHERE a.canonical_name='EBITDA' AND p.raw_label='2024-25';"),
    ("Rank years by Revenue from Operation",
     "SELECT p.raw_label AS period, f.value AS revenue FROM fact_pnl_annual f JOIN dim_account a ON f.account_id=a.account_id JOIN dim_period p ON p.period_id=f.period_id WHERE a.canonical_name='Revenue from Operation' ORDER BY revenue DESC;"),
    ("Year over year growth in EBITDA between 2023-24 and 2024-25",
     "WITH vals AS (SELECT p.raw_label AS period, f.value FROM fact_pnl_annual f JOIN dim_account a ON f.account_id=a.account_id JOIN dim_period p ON p.period_id=f.period_id WHERE a.canonical_name='EBITDA' AND p.raw_label IN ('2023-24','2024-25')) SELECT v1.period AS current_period, v1.value AS current_ebitda, v0.value AS prior_ebitda, (v1.value - v0.value)/v0.value AS yoy_growth FROM vals v1 JOIN vals v0 ON v0.period='2023-24' WHERE v1.period='2024-25';")
]

class LLMStrategy:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        # Attempt to load .env first if environment variables not set
        provider_env = os.getenv('LLM_PROVIDER')
        api_key_env = os.getenv('OPENAI_API_KEY')
        
        if not provider_env or not api_key_env:
            try:
                from dotenv import load_dotenv
                env_path = ROOT / '.env'
                if env_path.exists():
                    load_dotenv(dotenv_path=env_path, override=False)
            except Exception:
                pass
        
        self.provider = os.getenv('LLM_PROVIDER', 'none').lower()
        self.api_key = os.getenv('OPENAI_API_KEY')
        # Allow custom base URL for Azure/OpenAI proxies
        self.base_url = os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')

    def available(self) -> bool:
        return self.provider == 'openai' and bool(self.api_key)

    def _schema_summary(self) -> str:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        out = []
        for (t,) in tables:
            if t.startswith('sqlite_'): continue
            cols = cur.execute(f"PRAGMA table_info({t})").fetchall()
            col_names = [c[1] for c in cols]
            out.append(f"{t}({', '.join(col_names)})")
        conn.close()
        return "\n".join(out)

    def _build_prompt(self, question: str) -> str:
        schema = self._schema_summary()
        examples_text = "\n".join([f"Q: {q}\nSQL: {sql}" for q, sql in EXAMPLES])
        account_table_hints = (
            "Canonical table mapping hints:\n"
            "EBIT -> fact_roce_external (not balance sheet)\n"
            "Average capital employed -> fact_roce_external\n"
            "Revenue from Operation -> fact_pnl_annual\n"
            "EBITDA -> fact_pnl_annual\n"
            "EBITDA Margin -> view_ebitda_margin (period, ebitda_margin)\n"
        )
        return (
            "You are a strict SQLite SQL generator. Return ONLY one SQL statement. "
            "Rules: Use only tables listed. No modification statements. Avoid SELECT *. Parameterize literal years directly. "
            "Prefer correct fact tables per canonical mapping; do NOT use fact_balance_sheet for EBIT or average capital employed.\n"
            f"Schema:\n{schema}\n\n{account_table_hints}\nExamples:\n{examples_text}\n\nQuestion: {question}\nSQL:" )

    def generate_sql(self, question: str) -> Tuple[Optional[str], str]:
        if not self.available():
            return None, 'UNAVAILABLE'
        prompt = self._build_prompt(question)
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": "Generate safe SQL."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        debug = bool(os.getenv('LLM_DEBUG'))
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=headers, data=json.dumps(payload), timeout=30)
            if resp.status_code != 200:
                return None, f"HTTP_{resp.status_code}"
            data = resp.json()
            sql_text = data.get('choices',[{}])[0].get('message',{}).get('content','')
        except Exception:
            return None, 'ERROR'
        if not sql_text:
            return None, 'EMPTY'
        # Extract SQL - handle both single and multi-line responses
        sql = sql_text.strip()
        # Remove markdown code fences if present
        if sql.startswith('```'):
            sql = '\n'.join(sql.split('\n')[1:-1]).strip()
        # Join multi-line SQL into single line
        sql = ' '.join(sql.split())
        if debug:
            print("LLM_DEBUG_RAW_RESPONSE:", repr(sql_text))
            print("LLM_DEBUG_PROCESSED_SQL:", sql)
        # Canonical name sanitation fixes (LLM often shortens canonical names)
        replacements = [
            ("canonical_name='Revenue'", "canonical_name='Revenue from Operation'"),
            ("canonical_name=\"Revenue\"", "canonical_name='Revenue from Operation'"),
            ("canonical_name='EBITDA Margin'", "canonical_name IN ('EBITDA Margin','EBITDA %','EBIDTA %')"),
            ("canonical_name=\"EBITDA Margin\"", "canonical_name IN ('EBITDA Margin','EBITDA %','EBIDTA %')"),
        ]
        for old, new in replacements:
            if old in sql:
                sql = sql.replace(old, new)
        # Reject misuse of fact_balance_sheet for EBIT / capital employed
        if ("fact_balance_sheet" in sql and ("EBIT" in sql or "Average capital employed" in sql)):
            return None, 'BAD_TABLE'
        if not SAFE_SQL_REGEX.match(sql) or FORBIDDEN_REGEX.search(sql):
            return None, 'UNSAFE'
        schema = self._schema_summary()
        known_tables = {line.split('(')[0] for line in schema.splitlines()}
        
        # Extract CTE names from WITH clauses to exclude from validation
        cte_names = set()
        if sql.upper().startswith('WITH'):
            # Find all CTE names: "WITH name AS (...), name2 AS (...)"
            cte_pattern = re.findall(r'\bWITH\s+(\w+)\s+AS|,\s*(\w+)\s+AS', sql, re.IGNORECASE)
            cte_names = {name for pair in cte_pattern for name in pair if name}
        
        # Remove string literals first to avoid false matches
        sql_no_strings = re.sub(r"'[^']*'", '', sql)
        pattern_tables = re.findall(r"FROM\s+(\w+)|JOIN\s+(\w+)", sql_no_strings, re.IGNORECASE)
        flat_tables = {t for pair in pattern_tables for t in pair if t}
        
        # Exclude CTE names from validation - they're not real tables
        tables_to_validate = flat_tables - cte_names
        
        if debug:
            print(f"LLM_DEBUG_KNOWN_TABLES: {known_tables}")
            print(f"LLM_DEBUG_CTE_NAMES: {cte_names}")
            print(f"LLM_DEBUG_FOUND_TABLES: {flat_tables}")
            print(f"LLM_DEBUG_TABLES_TO_VALIDATE: {tables_to_validate}")
            print(f"LLM_DEBUG_TABLES_VALID: {tables_to_validate.issubset(known_tables)}")
        if not tables_to_validate.issubset(known_tables):
            return None, 'UNKNOWN_TABLE'
        return sql, 'OK'
