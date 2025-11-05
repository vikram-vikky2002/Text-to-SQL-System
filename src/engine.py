from pathlib import Path
import sqlite3
from typing import Tuple, Any, List
from .nl2sql import NL2SQLParser, ROOT
from .llm_strategy import LLMStrategy
from . import ingest
import math

class QAEngine:
    def __init__(self, db_path: Path = None):
        self.db_path = db_path or (ROOT / 'financial.db')
        if not self.db_path.exists():
            ingest.build(self.db_path)
        self.parser = NL2SQLParser(self.db_path)
        # Initialize optional LLM strategy
        self.llm = LLMStrategy(self.db_path)

    def answer(self, question: str) -> Tuple[str, str, str]:
        """Return (answer, status, method) where method is 'LLM' or 'heuristic'."""
        lower_q = question.lower()
        method = 'heuristic'
        # Deterministic YOY growth handling
        yoy_match = None
        if 'year over year growth' in lower_q or 'yoy growth' in lower_q:
            yoy_match = True
        if yoy_match:
            growth_resp = self._yoy_growth(question)
            if growth_resp:
                return growth_resp, 'OK', method
        # Capital employed vs EBIT comparative trend heuristic (preempts LLM)
        if ('average capital employed' in lower_q and 'ebit' in lower_q and any(k in lower_q for k in ['trend','change','compare','explain'])):
            cap_resp = self._compare_capital_ebit_trend(question)
            if cap_resp:
                return cap_resp, 'OK', method
        # Multi-metric combined query detection
        multi_metric = (("ebitda" in lower_q and "revenue" in lower_q) or 'summary' in lower_q or 'performance' in lower_q or 'ebitda margin' in lower_q)
        # Revenue + EBITDA margin trend comparison (heuristic)
        if ('compare' in lower_q or 'trend' in lower_q) and 'revenue' in lower_q and 'margin' in lower_q:
            comp = self._compare_revenue_margin_trend(question)
            if comp:
                return comp, 'OK', method
        # Allow LLM on comparative growth queries even if multi-metric terms present
        if multi_metric and not any(k in lower_q for k in ['growth','compare','change','trend']):
            return self._multi_metric(question), 'OK', method
        # Restrict LLM usage to analytical comparative queries (exclude pure rank/top/per formatting)
        use_llm = any(k in lower_q for k in ['growth','compare','change','trend','ratio','correlation','forecast','explain']) and self.llm.available()
        # Direct heuristic correlation override
        if 'correlation' in lower_q and 'revenue' in lower_q and ('margin' in lower_q or 'ebitda' in lower_q):
            corr = self._correlate_revenue_margin(question)
            if corr:
                return corr, 'OK', method
        if use_llm:
            llm_sql, llm_status = self.llm.generate_sql(question)
            if llm_status == 'OK' and llm_sql:
                with sqlite3.connect(self.db_path) as conn:
                    cur = conn.cursor()
                    try:
                        cur.execute(llm_sql)
                        rows = cur.fetchall()
                        method = 'LLM'
                        return self._format(question, rows), 'OK', method
                    except Exception:
                        pass  # fallback
        sql_bundle, status = self.parser.build_sql(question)
        if status == 'NO_ACCOUNT':
            return "I'm sorry, I can only answer questions about company finance and cargo operations contained in the provided dataset.", 'FAIL', method
        if isinstance(sql_bundle, tuple):
            sql, params = sql_bundle
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute(sql, params)
                rows = cur.fetchall()
        else:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute(sql_bundle)
                rows = cur.fetchall()
        response = self._format(question, rows)
        return response, 'OK', method

    def _format(self, question: str, rows: list) -> str:
        if not rows:
            return "No matching data found for the requested criteria."
        q_lower = question.lower()
        # Deduplicate rows by identical period/value for margin or roce trend noise
        if ('margin' in q_lower or 'roce' in q_lower) and len(rows) > 1 and all(len(r) >= 2 for r in rows):
            seen = {}
            for r in rows:
                key = (r[0], r[1])
                if key not in seen:
                    seen[key] = r
            rows = list(seen.values())
        # Port ranking formatting
        if (('top' in q_lower or 'rank' in q_lower) and 'port' in q_lower) and len(rows) > 1 and len(rows[0]) == 2:
            parts = []
            for port, val in rows:
                if val is None:
                    continue
                parts.append(f"{port} ({round(val,2)})")
            return "Top ports by EBIT: " + ", ".join(parts)
        # Cargo volume listing by port
        if ('port' in q_lower and 'volume' in q_lower) and len(rows) > 1 and len(rows[0]) == 2:
            parts = []
            for port, val in rows:
                if val is None:
                    continue
                parts.append(f"{port}: {round(val,2)}")
            return "Cargo volumes by port: " + ", ".join(parts)
        # Simple formatting rules
        if len(rows) == 1 and len(rows[0]) == 2:
            period, value = rows[0]
            if value is None:
                return f"Data for {period} is unavailable." 
            return f"In {period}, the value is {round(value, 2)}."
        # Multi-period summary
        parts = []
        for r in rows[:6]:  # limit verbosity
            period = r[0]
            val = r[1]
            if val is None:
                continue
            parts.append(f"{period}: {round(val,2)}")
        return "; ".join(parts)

    def _multi_metric(self, question: str) -> str:
        """Combined Revenue, EBITDA, EBITDA Margin for specified or latest period(s)."""
        parser = self.parser
        periods, single_years = parser.detect_periods(question)
        latest = parser.latest_period()
        target_periods = periods or ([] if single_years else [latest])
        placeholders = ','.join(['?'] * len(target_periods)) if target_periods else ''
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            # Select single canonical account_id (MIN) for each metric to avoid duplicate summation
            sql = (
                "SELECT p.raw_label, "
                "(SELECT f1.value FROM fact_pnl_annual f1 JOIN dim_account a1 ON f1.account_id=a1.account_id "
                " WHERE a1.canonical_name='Revenue from Operation' AND a1.account_id=(SELECT MIN(account_id) FROM dim_account WHERE canonical_name='Revenue from Operation') "
                " AND f1.period_id=p.period_id) AS revenue, "
                "(SELECT f2.value FROM fact_pnl_annual f2 JOIN dim_account a2 ON f2.account_id=a2.account_id "
                " WHERE a2.canonical_name='EBITDA' AND a2.account_id=(SELECT MIN(account_id) FROM dim_account WHERE canonical_name='EBITDA') "
                " AND f2.period_id=p.period_id) AS ebitda "
                "FROM dim_period p"
            )
            params: List[str] = []
            if target_periods:
                sql += f" WHERE p.raw_label IN ({placeholders})"
                params.extend(target_periods)
            sql += " ORDER BY p.sort_key DESC"
            cur.execute(sql, params)
            rows = cur.fetchall()
        if not rows:
            return "No data available for the requested periods."
        parts = []
        for raw_label, revenue, ebitda in rows:
            margin = (ebitda / revenue) if revenue else None
            parts.append(f"{raw_label}: Revenue {round(revenue,2)}; EBITDA {round(ebitda,2)}; Margin {round(margin,2) if margin is not None else 'n/a'}")
        return " | ".join(parts)

    def _yoy_growth(self, question: str) -> str:
        """Compute YOY growth for a single account between two explicit periods."""
        periods, _ = self.parser.detect_periods(question)
        if len(periods) < 2:
            return "Specify two fiscal periods for year over year growth."
        target_periods = periods[:2]
        lower_q = question.lower()
        accounts = self.parser.detect_accounts(question)
        # Fallback: manually look for EBITDA, revenue tokens
        if not accounts:
            if 'ebitda' in lower_q:
                accounts = ['EBITDA']
            elif 'revenue' in lower_q:
                accounts = ['Revenue from Operation']
        if len(accounts) != 1:
            return None
        account = accounts[0]
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            sql = (
                "SELECT p.raw_label, SUM(f.value) AS value FROM fact_pnl_annual f "
                "JOIN dim_account a ON f.account_id=a.account_id "
                "JOIN dim_period p ON p.period_id=f.period_id "
                "WHERE a.canonical_name=? AND p.raw_label IN (?, ?) "
                "GROUP BY p.raw_label" )
            cur.execute(sql, (account, target_periods[0], target_periods[1]))
            rows = cur.fetchall()
        if len(rows) != 2:
            return "Data unavailable for requested periods." if rows else "No data found."
        # Map by period
        val_map = {r[0]: r[1] for r in rows}
        p0, p1 = target_periods[0], target_periods[1]
        v0, v1 = val_map.get(p0), val_map.get(p1)
        if v0 is None or v1 is None or v0 == 0:
            return f"Insufficient data to compute YOY growth for {account}."
        growth = (v1 - v0) / v0
        return f"{account} YOY growth from {p0} to {p1}: {round(growth*100,2)}% (from {round(v0,2)} to {round(v1,2)})."

    def _compare_revenue_margin_trend(self, question: str) -> str:
        """Return revenue and EBITDA margin for last N years (default 4) in one line."""
        lower_q = question.lower()
        periods, single_years = self.parser.detect_periods(question)
        # Determine N from 'last N years'
        import re as _re
        m = _re.search(r'last\s+(\d+)\s+years', lower_q)
        n = int(m.group(1)) if m else 4
        # Fetch latest n periods by sort_key
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT raw_label FROM dim_period WHERE sort_key IS NOT NULL ORDER BY sort_key DESC")
            all_periods = [r[0] for r in cur.fetchall()]
        target = all_periods[:n]
        if not target:
            return None
        placeholders = ','.join(['?'] * len(target))
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            # Revenue values
            cur.execute(
                f"SELECT p.raw_label, f.value FROM fact_pnl_annual f JOIN dim_account a ON f.account_id=a.account_id JOIN dim_period p ON p.period_id=f.period_id "
                f"WHERE a.canonical_name='Revenue from Operation' AND p.raw_label IN ({placeholders}) AND a.account_id=(SELECT MIN(account_id) FROM dim_account WHERE canonical_name='Revenue from Operation')",
                target)
            rev_rows = {r[0]: r[1] for r in cur.fetchall()}
            # Margin values from view
            cur.execute(f"SELECT period, ebitda_margin FROM view_ebitda_margin WHERE period IN ({placeholders})", target)
            margin_rows = {r[0]: r[1] for r in cur.fetchall()}
        parts = []
        for p in target:
            r = rev_rows.get(p)
            mrg = margin_rows.get(p)
            if r is None or mrg is None:
                continue
            parts.append(f"{p}: Revenue {round(r,2)}; EBITDA Margin {round(mrg,2)}")
        return " | ".join(parts) if parts else None

    def _correlate_revenue_margin(self, question: str) -> str:
        """Compute correlation between revenue YoY growth and EBITDA margin delta for available consecutive years."""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT p.raw_label, f.value FROM fact_pnl_annual f JOIN dim_account a ON f.account_id=a.account_id JOIN dim_period p ON p.period_id=f.period_id WHERE a.canonical_name='Revenue from Operation' AND a.account_id=(SELECT MIN(account_id) FROM dim_account WHERE canonical_name='Revenue from Operation') ORDER BY p.sort_key ASC")
            rev_rows = cur.fetchall()
            cur.execute("SELECT period, ebitda_margin FROM view_ebitda_margin ORDER BY period ASC")
            margin_rows = cur.fetchall()
        rev_map = {p: v for p, v in rev_rows if v is not None}
        margin_map = {p: m for p, m in margin_rows if m is not None}
        periods = sorted(set(rev_map.keys()) & set(margin_map.keys()))
        if len(periods) < 3:
            return "Insufficient data for correlation analysis."
        # Compute growth and margin deltas across consecutive periods
        growth_vals = []
        margin_deltas = []
        prev_rev = None
        prev_margin = None
        prev_period = None
        for p in periods:
            r = rev_map[p]; m = margin_map[p]
            if prev_rev is not None and prev_rev != 0 and prev_margin is not None:
                growth = (r - prev_rev) / prev_rev
                delta = m - prev_margin
                growth_vals.append(growth)
                margin_deltas.append(delta)
            prev_rev = r; prev_margin = m; prev_period = p
        if len(growth_vals) < 2:
            return "Insufficient consecutive periods for correlation."
        # Pearson correlation
        import math
        n = len(growth_vals)
        mean_g = sum(growth_vals)/n
        mean_m = sum(margin_deltas)/n
        cov = sum((g - mean_g)*(d - mean_m) for g, d in zip(growth_vals, margin_deltas))
        var_g = sum((g - mean_g)**2 for g in growth_vals)
        var_m = sum((d - mean_m)**2 for d in margin_deltas)
        if var_g == 0 or var_m == 0:
            return "No variation to compute correlation."
        corr = cov / math.sqrt(var_g * var_m)
        # Simple interpretation
        if corr > 0.5:
            interp = "moderately positive"
        elif corr > 0.2:
            interp = "weak positive"
        elif corr < -0.5:
            interp = "moderately negative"
        elif corr < -0.2:
            interp = "weak negative"
        else:
            interp = "little"
        return f"Correlation between revenue YoY growth and EBITDA margin change: {round(corr,3)} ({interp} relationship)."

    def _compare_capital_ebit_trend(self, question: str) -> str:
        """Compare Average capital employed and EBIT over last N years with ROCE inline."""
        lower_q = question.lower()
        import re as _re
        m = _re.search(r'last\s+(\d+)\s+years', lower_q)
        n = int(m.group(1)) if m else 4
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            # Aggregate per period to avoid duplicate entity/row explosions
            cur.execute("""
                SELECT p.raw_label, SUM(f.value) AS cap_employed
                FROM fact_roce_external f
                JOIN dim_account a ON f.account_id=a.account_id
                JOIN dim_period p ON p.period_id=f.period_id
                WHERE a.canonical_name='Average capital employed'
                GROUP BY p.period_id
                ORDER BY p.sort_key DESC
            """)
            cap_rows = cur.fetchall()
            cur.execute("""
                SELECT p.raw_label, SUM(f.value) AS ebit_total
                FROM fact_roce_external f
                JOIN dim_account a ON f.account_id=a.account_id
                JOIN dim_period p ON p.period_id=f.period_id
                WHERE a.canonical_name='EBIT'
                GROUP BY p.period_id
                ORDER BY p.sort_key DESC
            """)
            ebit_rows = cur.fetchall()
        cap_map = {p: v for p, v in cap_rows if v is not None}
        ebit_map = {p: v for p, v in ebit_rows if v is not None}
        # Intersect periods while preserving order
        ordered_periods = [p for p, _ in cap_rows if p in ebit_map][:n]
        periods = ordered_periods
        if not periods:
            return "No data available to compare EBIT and average capital employed."
        parts = []
        for p in periods:
            cap_val = cap_map.get(p)
            ebit_val = ebit_map.get(p)
            if cap_val is None or ebit_val is None or cap_val == 0:
                roce = 'n/a'
            else:
                roce = round(ebit_val / cap_val, 3)
            parts.append(f"{p}: EBIT {round(ebit_val,2)}; Avg Cap Empl {round(cap_val,2)}; ROCE {roce}")
        return " | ".join(parts)

    def close(self):
        self.parser.close()
