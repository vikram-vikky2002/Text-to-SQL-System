import re
import sqlite3
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parent.parent

class NL2SQLParser:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.synonyms = self._load_synonyms()

    def _load_synonyms(self):
        with open(ROOT / 'src' / 'data_dictionary.yaml', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _canonical_account(self, text: str):
        lowered = text.lower()
        for canonical, syns in self.synonyms.get('synonyms', {}).items():
            for s in syns:
                if lowered == s.lower():
                    return canonical
        return None

    def detect_accounts(self, question: str):
        tokens = re.split(r'[^a-zA-Z0-9%]+', question.lower())
        found = set()
        for canonical, syns in self.synonyms.get('synonyms', {}).items():
            for s in syns:
                words = s.lower().split()
                if all(w in tokens for w in words):
                    found.add(canonical)
        return list(found)

    def detect_periods(self, question: str):
        # Find explicit fiscal labels
        periods = re.findall(r'(20\d{2}-\d{2})', question)
        single_years = re.findall(r'(20\d{2})', question)
        return periods, single_years

    def latest_period(self):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT raw_label FROM dim_period WHERE sort_key IS NOT NULL ORDER BY sort_key DESC LIMIT 1")
            row = cur.fetchone()
            return row[0] if row else None

    def build_sql(self, question: str):
        accounts = self.detect_accounts(question)
        periods, single_years = self.detect_periods(question)
        group_by_port = 'by port' in question.lower() or 'per port' in question.lower()
        top_n_match = re.search(r'top\s+(\d+)', question.lower())
        top_n = int(top_n_match.group(1)) if top_n_match else None
        latest = self.latest_period()
        q_lower = question.lower()
        rank_years = 'rank years' in q_lower or ('rank' in q_lower and 'years' in q_lower)
        all_years = 'all years' in q_lower or 'each year' in q_lower or 'every year' in q_lower
        rank_ports = ('top' in q_lower or 'rank' in q_lower) and 'port' in q_lower
        # If ranking or explicit all-years request, do not restrict to latest
        if rank_years or all_years:
            target_periods = None  # fetch all
        else:
            target_periods = periods or ([] if single_years else [latest])
        # Explicit cargo volume by port (overrides account detection)
        if 'by port' in q_lower and ('cargo' in q_lower or 'volume' in q_lower):
            cargo_types = ['dry','crude','liquid','container','cars']
            cargo_filter = None
            for ct in cargo_types:
                if ct in q_lower:
                    cargo_filter = ct.capitalize()
                    break
            target_period = periods[0] if periods else latest
            if not target_period:
                return None, 'NO_ACCOUNT'
            sql = (
                "SELECT dp.port_name, SUM(v.volume_value) AS volume FROM fact_volume v "
                "JOIN dim_port dp ON dp.port_id=v.port_id "
                "JOIN dim_period p ON p.period_id=v.period_id "
                "JOIN dim_cargo_type ct ON ct.cargo_type_id=v.cargo_type_id "
                "WHERE p.raw_label=? " + ("AND ct.name=? " if cargo_filter else "") +
                "GROUP BY dp.port_name ORDER BY volume DESC",
                [target_period] + ([cargo_filter] if cargo_filter else [])
            )
            return sql, 'OK'
        # Cargo volume by port queries even if no canonical account matched
        if (not accounts) and 'port' in q_lower and ('cargo' in q_lower or 'volume' in q_lower):
            cargo_types = ['dry','crude','liquid','container','cars']
            cargo_filter = None
            for ct in cargo_types:
                if ct in q_lower:
                    cargo_filter = ct.capitalize()
                    break
            target_period = periods[0] if periods else latest
            if not target_period:
                return None, 'NO_ACCOUNT'
            sql = (
                "SELECT dp.port_name, SUM(v.volume_value) AS volume FROM fact_volume v "
                "JOIN dim_port dp ON dp.port_id=v.port_id "
                "JOIN dim_period p ON p.period_id=v.period_id "
                "JOIN dim_cargo_type ct ON ct.cargo_type_id=v.cargo_type_id "
                "WHERE p.raw_label=? " + ("AND ct.name=? " if cargo_filter else "") +
                "GROUP BY dp.port_name ORDER BY volume DESC",
                [target_period] + ([cargo_filter] if cargo_filter else [])
            )
            return sql, 'OK'
        if not accounts:
            return None, 'NO_ACCOUNT'
        # Simplified: handle single account queries
        account = accounts[0]
        # Port ranking branch (currently supports only EBIT canonical)
        if rank_ports and account == 'EBIT':
            # Require a single period; fall back to latest if none provided
            target_period = periods[0] if periods else latest
            limit_clause = f"LIMIT {top_n}" if top_n else "LIMIT 3"
            # Aggregate EBIT per port across ROCE categories
            sql = (
                "SELECT dp.port_name, SUM(fri.value) AS ebit FROM fact_roce_internal fri "
                "JOIN dim_account a ON fri.account_id=a.account_id AND a.canonical_name='EBIT' "
                "JOIN dim_period p ON p.period_id=fri.period_id "
                "JOIN dim_port dp ON dp.port_id=fri.port_id "
                "WHERE p.raw_label=? "
                "GROUP BY dp.port_name ORDER BY ebit DESC " + limit_clause,
                [target_period]
            )
            return sql, 'OK'
        # Port EBIT per MMT
        if 'ebit per mmt' in q_lower or 'ebitda per mmt' in q_lower or 'ebit per cargo' in q_lower:
            # Use view_port_ebit_volume
            target_period = periods[0] if periods else latest
            sql = ("SELECT port_name, ebit_per_mmt FROM view_port_ebit_volume WHERE period=? ORDER BY ebit_per_mmt DESC", [target_period])
            return sql, 'OK'
        # Determine source fact table heuristically
        table_map = {
            'EBITDA': 'fact_pnl_annual',
            'Revenue from Operation': 'fact_pnl_annual',
            'EBIT': 'fact_roce_external',
            'Average capital employed': 'fact_roce_external',
            'Total Cargo (MMT)': 'fact_pnl_quarterly',  # Placeholder if appears there
        }
        fact_table = table_map.get(account, 'fact_pnl_annual')
        sql = None
        if 'margin' in question.lower() and 'ebitda' in question.lower():
            sql = "SELECT period, ebitda_margin FROM view_ebitda_margin"
        elif account == 'EBIT' and 'roce' in question.lower():
            sql = "SELECT period, roce FROM view_roce"
        else:
            # Select a single stable account_id for the canonical name to avoid double counting
            base = (
                f"SELECT p.raw_label AS period, f.value AS value "
                f"FROM {fact_table} f "
                f"JOIN dim_account a ON f.account_id=a.account_id "
                f"JOIN dim_period p ON p.period_id=f.period_id "
                f"WHERE a.canonical_name=? AND a.account_id = (SELECT MIN(account_id) FROM dim_account WHERE canonical_name=?)"
            )
            params = [account, account]
            if target_periods:
                placeholders = ','.join(['?'] * len(target_periods))
                base += f" AND p.raw_label IN ({placeholders})"
                params.extend(target_periods)
            # decide ordering
            order_clause = "ORDER BY p.sort_key DESC"
            if rank_years:
                order_clause = "ORDER BY value DESC"
            base += f" GROUP BY p.raw_label, p.sort_key {order_clause}"
            if top_n and ('top' in question.lower() and 'port' not in question.lower()):
                # Top periods by value
                base = base.replace("ORDER BY p.sort_key DESC", "ORDER BY value DESC") + f" LIMIT {top_n}"
            sql = (base, params)
        return sql, 'OK'

    def close(self):
        # No persistent connection to close in stateless mode
        return
