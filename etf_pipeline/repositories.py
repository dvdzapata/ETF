"""Repository layer encapsulating database access."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Iterable, List, Tuple

from .db import Database

logger = logging.getLogger(__name__)


class SymbolRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def list_symbols(self) -> list[dict]:
        query = "SELECT id, symbol FROM etf_symbols ORDER BY symbol"
        return self.db.fetchall(query)

    def existing_symbols(self) -> set[str]:
        query = "SELECT symbol FROM etf_symbols"
        rows = self.db.fetchall(query)
        return {row["symbol"] for row in rows}

    def insert_symbols(self, symbols: Iterable[dict]) -> list[str]:
        query = (
            "INSERT INTO etf_symbols (symbol, nombre, mercado, moneda) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (symbol) DO NOTHING"
        )
        params: list[Tuple[str | None, str | None, str | None, str | None]] = []
        inserted_symbols: list[str] = []
        for item in symbols:
            symbol = item.get("symbol")
            if not symbol:
                continue
            params.append(
                (
                    symbol,
                    item.get("name"),
                    item.get("exchange"),
                    item.get("currency"),
                )
            )
            inserted_symbols.append(symbol)
        if params:
            self.db.execute_batch(query, params)
        return inserted_symbols

    def update_symbol_profiles(self, profiles: Iterable[dict]) -> None:
        query = (
            "UPDATE etf_symbols SET "
            "nombre = %s, "
            "mercado = %s, "
            "isin = %s, "
            "figi = %s, "
            "cik = %s, "
            "descripcion = %s, "
            "assetclass = %s, "
            "cusip = %s, "
            "gestora_etf = %s, "
            "expenseratio = %s, "
            "total_acciones = %s, "
            "volumen_medio = %s, "
            "alta = %s, "
            "nav = %s, "
            "moneda = %s, "
            "total_holdings = %s, "
            "updatedat = %s "
            "WHERE symbol = %s"
        )
        params: list[Tuple] = []
        for profile in profiles:
            symbol = profile.get("symbol")
            if not symbol:
                continue
            params.append(
                (
                    profile.get("nombre"),
                    profile.get("mercado"),
                    profile.get("isin"),
                    profile.get("figi"),
                    profile.get("cik"),
                    profile.get("descripcion"),
                    profile.get("assetclass"),
                    profile.get("cusip"),
                    profile.get("gestora_etf"),
                    profile.get("expenseratio"),
                    profile.get("total_acciones"),
                    profile.get("volumen_medio"),
                    profile.get("alta"),
                    profile.get("nav"),
                    profile.get("moneda"),
                    profile.get("total_holdings"),
                    profile.get("updatedat"),
                    symbol,
                )
            )
        if params:
            self.db.execute_batch(query, params)


class TimeSeriesRepository:
    def __init__(self, db: Database, table: str) -> None:
        self.db = db
        self.table = table

    def get_last_date(self, symbol_id: int) -> date | datetime | None:
        query = f"SELECT MAX(date) AS max_date FROM {self.table} WHERE id = %s"
        row = self.db.fetchone(query, (symbol_id,))
        if row and row.get("max_date"):
            return row["max_date"]
        return None

    def insert_rows(self, columns: List[str], rows: Iterable[Tuple]) -> None:
        rows_list = list(rows)
        if not rows_list:
            return
        placeholders = ", ".join(["%s"] * len(columns))
        columns_clause = ", ".join(columns)
        query = f"INSERT INTO {self.table} ({columns_clause}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        self.db.execute_batch(query, rows_list)


class SnapshotRepository:
    def __init__(self, db: Database, table: str, key_columns: list[str]) -> None:
        self.db = db
        self.table = table
        self.key_columns = key_columns

    def replace_snapshot(self, symbol_id: int, symbol: str, rows: Iterable[dict], columns: list[str]) -> None:
        with self.db.cursor() as cur:
            placeholders = ", ".join(["%s"] * len(columns))
            columns_clause = ", ".join(columns)
            delete_query = f"DELETE FROM {self.table} WHERE id = %s"
            cur.execute(delete_query, (symbol_id,))
            insert_query = f"INSERT INTO {self.table} ({columns_clause}) VALUES ({placeholders})"
            for row in rows:
                cur.execute(insert_query, tuple(row[col] for col in columns))


class HoldingsRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def replace_holdings(self, symbol_id: int, symbol: str, rows: Iterable[dict]) -> None:
        with self.db.cursor() as cur:
            cur.execute("DELETE FROM etf_holdings WHERE symbol = %s", (symbol,))
            query = (
                "INSERT INTO etf_holdings (symbol, id, asset, name, isin, security_cusip, shares_number, weight_percentage, market_value, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            for row in rows:
                cur.execute(
                    query,
                    (
                        symbol,
                        row["id"],
                        row.get("asset"),
                        row.get("name"),
                        row.get("isin"),
                        row.get("security_cusip"),
                        row.get("shares_number"),
                        row.get("weight_percentage"),
                        row.get("market_value"),
                        row.get("updated_at"),
                    ),
                )


class RiskRepositories:
    def __init__(self, db: Database) -> None:
        self.db = db

    def upsert_country_risk(self, rows: Iterable[dict]) -> None:
        query = (
            "INSERT INTO riesgo_pais (country, continent, countryRiskPremium, totalEquityRiskPremium) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (country) DO UPDATE SET continent = EXCLUDED.continent, countryRiskPremium = EXCLUDED.countryRiskPremium, totalEquityRiskPremium = EXCLUDED.totalEquityRiskPremium"
        )
        params = [
            (
                row.get("country"),
                row.get("continent"),
                row.get("countryRiskPremium"),
                row.get("totalEquityRiskPremium"),
            )
            for row in rows
        ]
        self.db.execute_batch(query, params)

    def upsert_industry_risk(self, rows: Iterable[dict]) -> None:
        query = (
            "INSERT INTO riesgo_industria (industry, average_change) VALUES (%s, %s) "
            "ON CONFLICT (industry) DO UPDATE SET average_change = EXCLUDED.average_change"
        )
        params = [(row.get("industry"), row.get("average_change")) for row in rows]
        self.db.execute_batch(query, params)


__all__ = [
    "SymbolRepository",
    "TimeSeriesRepository",
    "SnapshotRepository",
    "HoldingsRepository",
    "RiskRepositories",
]
