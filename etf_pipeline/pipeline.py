"""Core orchestration for the ETF data ingestion pipeline."""
from __future__ import annotations

import concurrent.futures
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable

from .config import PipelineSettings
from .http_client import FMPClient
from .repositories import (
    HoldingsRepository,
    RiskRepositories,
    SnapshotRepository,
    SymbolRepository,
    TimeSeriesRepository,
)
from .state import DownloadState
from .validators import filter_records, in_range, is_valid_date, parse_decimal

logger = logging.getLogger(__name__)
rejection_logger = logging.getLogger("rejections")


def _to_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_date_value(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError:
            try:
                return datetime.strptime(text, "%Y-%m-%d").date()
            except ValueError:
                return None
    return None


def _parse_datetime_value(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


class ETLPipeline:
    def __init__(
        self,
        *,
        settings: PipelineSettings,
        client: FMPClient,
        symbol_repo: SymbolRepository,
        db_time_series: Dict[str, TimeSeriesRepository],
        country_repo: SnapshotRepository,
        industry_repo: SnapshotRepository,
        asset_repo: SnapshotRepository,
        holdings_repo: HoldingsRepository,
        risk_repo: RiskRepositories,
        state: DownloadState,
    ) -> None:
        self.settings = settings
        self.client = client
        self.symbol_repo = symbol_repo
        self.db_time_series = db_time_series
        self.country_repo = country_repo
        self.industry_repo = industry_repo
        self.asset_repo = asset_repo
        self.holdings_repo = holdings_repo
        self.risk_repo = risk_repo
        self.state = state

    def run(self) -> None:
        logger.info("Starting ETF pipeline run")
        self._sync_symbols()
        symbols = self.symbol_repo.list_symbols()
        logger.info("Processing %s symbols", len(symbols))
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.settings.max_workers) as executor:
            futures = [executor.submit(self._process_symbol, symbol) for symbol in symbols]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.exception("Unhandled error during symbol processing: %s", exc)
        self._update_market_risk()
        self._update_industry_snapshot()
        logger.info("Pipeline run completed")

    def _sync_symbols(self) -> None:
        logger.info("Synchronising ETF symbols from API")
        try:
            data = self.client.get_json("etf-list")
            if not isinstance(data, list):
                logger.error("Unexpected response for symbol list: %s", data)
                return
        except Exception:
            logger.exception("Failed to download ETF symbol list")
            return

        existing = self.symbol_repo.existing_symbols()
        new_symbols = [item for item in data if item.get("symbol") and item["symbol"] not in existing]
        if not new_symbols:
            logger.info("No new symbols found")
            return
        inserted_symbols = self.symbol_repo.insert_symbols(new_symbols)
        if not inserted_symbols:
            logger.info("No new symbols were persisted after filtering")
            return
        logger.info("Inserted %s new symbols", len(inserted_symbols))
        self._populate_symbol_profiles(inserted_symbols)

    def _populate_symbol_profiles(self, symbols: Iterable[str]) -> None:
        symbols_list = list(symbols)
        if not symbols_list:
            return
        logger.info("Downloading ETF profiles for %s symbols", len(symbols_list))
        profiles: list[dict] = []
        for symbol in symbols_list:
            try:
                data = self.client.get_json("etf/info", {"symbol": symbol})
            except Exception:  # pylint: disable=broad-except
                logger.exception("Failed to download profile for %s", symbol)
                continue
            if not isinstance(data, list) or not data:
                logger.warning("Unexpected profile response for %s: %s", symbol, data)
                continue
            record = data[0] or {}
            total_acciones_source = record.get("sharesOutstanding")
            if total_acciones_source is None:
                total_acciones_source = record.get("assetsUnderManagement")
            profile = {
                "symbol": symbol,
                "nombre": record.get("name"),
                "mercado": record.get("domicile") or record.get("exchange"),
                "isin": record.get("isin"),
                "figi": record.get("figi"),
                "cik": record.get("cik"),
                "descripcion": record.get("description"),
                "assetclass": record.get("assetClass"),
                "cusip": record.get("securityCusip"),
                "gestora_etf": record.get("etfCompany"),
                "expenseratio": parse_decimal(record.get("expenseRatio")),
                "total_acciones": parse_decimal(total_acciones_source),
                "volumen_medio": _to_int(record.get("avgVolume")),
                "alta": _parse_date_value(record.get("inceptionDate")),
                "nav": parse_decimal(record.get("nav")),
                "moneda": record.get("navCurrency") or record.get("currency"),
                "total_holdings": _to_int(record.get("holdingsCount")),
                "updatedat": _parse_datetime_value(record.get("updatedAt")),
            }
            profiles.append(profile)
        if profiles:
            self.symbol_repo.update_symbol_profiles(profiles)
            logger.info("Updated ETF profiles for %s symbols", len(profiles))

    def _process_symbol(self, symbol_row: dict) -> None:
        symbol_id = symbol_row["id"]
        symbol = symbol_row["symbol"]
        logger.info("Processing symbol %s (%s)", symbol, symbol_id)
        try:
            self._update_eod_prices(symbol_id, symbol)
            self._update_adjusted_prices(symbol_id, symbol)
            self._update_hourly_prices(symbol_id, symbol)
            self._update_dividends(symbol_id, symbol)
            self._update_country_exposure(symbol_id, symbol)
            self._update_industry_exposure(symbol_id, symbol)
            self._update_asset_exposure(symbol_id, symbol)
            self._update_holdings(symbol_id, symbol)
        except Exception:  # pylint: disable=broad-except
            logger.exception("Error processing symbol %s", symbol)
        else:
            logger.info("Completed symbol %s", symbol)

    def _update_eod_prices(self, symbol_id: int, symbol: str) -> None:
        repo = self.db_time_series["etf_cotizacion"]
        last_date = repo.get_last_date(symbol_id)
        params: Dict[str, Any] = {}
        if isinstance(last_date, date):
            params["from"] = (last_date + timedelta(days=1)).isoformat()
        endpoint = f"historical-price-eod/full"
        data = self.client.get_json(endpoint, {**params, "symbol": symbol})
        if not isinstance(data, dict) or "historical" not in data:
            logger.warning("Unexpected EOD response for %s: %s", symbol, data)
            return
        records = data.get("historical", [])
        filtered = self._validate_eod_records(symbol, records)
        rows = [
            (
                symbol,
                symbol_id,
                item["date"],
                parse_decimal(item.get("open")),
                parse_decimal(item.get("high")),
                parse_decimal(item.get("low")),
                parse_decimal(item.get("close")),
                _to_int(item.get("volume")),
                parse_decimal(item.get("change")),
                parse_decimal(item.get("changePercent")),
                parse_decimal(item.get("vwap")),
            )
            for item in filtered
        ]
        repo.insert_rows(
            [
                "symbol",
                "id",
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "change",
                "change_percent",
                "vwap",
            ],
            rows,
        )
        self.state.update_symbol_state(symbol, {"eod_last_date": rows[-1][2] if rows else last_date})

    def _validate_eod_records(self, symbol: str, records: Iterable[dict]) -> list[dict]:
        return filter_records(
            records,
            validators=[
                ("date", lambda value: isinstance(value, str) and is_valid_date(value), "Invalid date"),
            ],
            rejection_logger=rejection_logger,
            context={"symbol": symbol, "dataset": "etf_cotizacion"},
        )

    def _update_adjusted_prices(self, symbol_id: int, symbol: str) -> None:
        repo = self.db_time_series["etf_ohlc_ajustado"]
        last_date = repo.get_last_date(symbol_id)
        params: Dict[str, Any] = {}
        if isinstance(last_date, date):
            params["from"] = (last_date + timedelta(days=1)).isoformat()
        data = self.client.get_json("historical-price-eod/dividend-adjusted", {**params, "symbol": symbol})
        if not isinstance(data, dict) or "historical" not in data:
            logger.warning("Unexpected adjusted OHLC response for %s: %s", symbol, data)
            return
        records = data.get("historical", [])
        filtered = filter_records(
            records,
            validators=[("date", lambda value: isinstance(value, str) and is_valid_date(value), "Invalid date")],
            rejection_logger=rejection_logger,
            context={"symbol": symbol, "dataset": "etf_ohlc_ajustado"},
        )
        rows = [
            (
                symbol,
                symbol_id,
                item["date"],
                parse_decimal(item.get("adjOpen")),
                parse_decimal(item.get("adjHigh")),
                parse_decimal(item.get("adjLow")),
                parse_decimal(item.get("adjClose")),
                _to_int(item.get("volume")),
            )
            for item in filtered
        ]
        repo.insert_rows(
            ["symbol", "id", "date", "adj_open", "adj_high", "adj_low", "adj_close", "volume"],
            rows,
        )
        self.state.update_symbol_state(symbol, {"adjusted_last_date": rows[-1][2] if rows else last_date})

    def _update_hourly_prices(self, symbol_id: int, symbol: str) -> None:
        repo = self.db_time_series["etf_por_horas"]
        last_ts = repo.get_last_date(symbol_id)
        params: Dict[str, Any] = {}
        if isinstance(last_ts, datetime):
            params["from"] = (last_ts + timedelta(hours=1)).isoformat()
        data = self.client.get_json("historical-chart/1hour", {**params, "symbol": symbol})
        if not isinstance(data, list):
            logger.warning("Unexpected hourly response for %s: %s", symbol, data)
            return
        filtered = filter_records(
            data,
            validators=[("date", lambda value: isinstance(value, str) and is_valid_date(value), "Invalid datetime")],
            rejection_logger=rejection_logger,
            context={"symbol": symbol, "dataset": "etf_por_horas"},
        )
        rows = [
            (
                symbol,
                symbol_id,
                item["date"],
                parse_decimal(item.get("open")),
                parse_decimal(item.get("low")),
                parse_decimal(item.get("high")),
                parse_decimal(item.get("close")),
                _to_int(item.get("volume")),
            )
            for item in filtered
        ]
        repo.insert_rows(
            ["symbol", "id", "date", "open", "low", "high", "close", "volume"],
            rows,
        )
        self.state.update_symbol_state(symbol, {"hourly_last_ts": rows[-1][2] if rows else last_ts})

    def _update_dividends(self, symbol_id: int, symbol: str) -> None:
        repo = self.db_time_series["etf_dividendos"]
        last_date = repo.get_last_date(symbol_id)
        params: Dict[str, Any] = {}
        if isinstance(last_date, date):
            params["from"] = (last_date + timedelta(days=1)).isoformat()
        data = self.client.get_json("dividends", {**params, "symbol": symbol})
        if not isinstance(data, list):
            logger.warning("Unexpected dividend response for %s: %s", symbol, data)
            return
        filtered = filter_records(
            data,
            validators=[("date", lambda value: isinstance(value, str) and is_valid_date(value), "Invalid date")],
            rejection_logger=rejection_logger,
            context={"symbol": symbol, "dataset": "etf_dividendos"},
        )
        rows = []
        for item in filtered:
            yield_value = parse_decimal(item.get("yield"))
            if not in_range(yield_value, 0, 100):
                rejection_logger.warning(
                    "Rejected dividend yield for %s on %s: %s",
                    symbol,
                    item.get("date"),
                    item.get("yield"),
                )
                continue
            rows.append(
                (
                    symbol,
                    symbol_id,
                    item.get("date"),
                    item.get("recordDate"),
                    item.get("paymentDate"),
                    item.get("declarationDate"),
                    parse_decimal(item.get("adjDividend")),
                    parse_decimal(item.get("dividend")),
                    yield_value,
                    item.get("frequency"),
                )
            )
        repo.insert_rows(
            [
                "symbol",
                "id",
                "date",
                "record_date",
                "payment_date",
                "declaration_date",
                "adj_dividend",
                "dividend",
                "yield",
                "frequency",
            ],
            rows,
        )
        self.state.update_symbol_state(symbol, {"dividend_last_date": rows[-1][2] if rows else last_date})

    def _update_country_exposure(self, symbol_id: int, symbol: str) -> None:
        data = self.client.get_json("etf/country-weightings", {"symbol": symbol})
        if not isinstance(data, list):
            logger.warning("Unexpected country exposure response for %s: %s", symbol, data)
            return
        validated = []
        for item in data:
            weight = parse_decimal(item.get("weightPercentage"))
            if not in_range(weight, 0, 100):
                rejection_logger.warning(
                    "Rejected country weight for %s | country=%s | weight=%s",
                    symbol,
                    item.get("country"),
                    item.get("weightPercentage"),
                )
                continue
            validated.append(
                {
                    "symbol": symbol,
                    "id": symbol_id,
                    "country": item.get("country"),
                    "weight_percentage": weight,
                }
            )
        self.country_repo.replace_snapshot(symbol_id, symbol, validated, ["symbol", "id", "country", "weight_percentage"])

    def _update_industry_exposure(self, symbol_id: int, symbol: str) -> None:
        data = self.client.get_json("etf/sector-weightings", {"symbol": symbol})
        if not isinstance(data, list):
            logger.warning("Unexpected industry exposure response for %s: %s", symbol, data)
            return
        validated = []
        for item in data:
            weight = parse_decimal(item.get("weightPercentage"))
            if not in_range(weight, 0, 100):
                rejection_logger.warning(
                    "Rejected industry weight for %s | industry=%s | weight=%s",
                    symbol,
                    item.get("sector"),
                    item.get("weightPercentage"),
                )
                continue
            validated.append(
                {
                    "symbol": symbol,
                    "id": symbol_id,
                    "industry": item.get("sector") or item.get("asset"),
                    "weight_percentage": weight,
                }
            )
        self.industry_repo.replace_snapshot(symbol_id, symbol, validated, ["symbol", "id", "industry", "weight_percentage"])

    def _update_asset_exposure(self, symbol_id: int, symbol: str) -> None:
        data = self.client.get_json("etf/asset-exposure", {"symbol": symbol})
        if not isinstance(data, list):
            logger.warning("Unexpected asset exposure response for %s: %s", symbol, data)
            return
        validated = []
        for item in data:
            weight = parse_decimal(item.get("weightPercentage"))
            if not in_range(weight, 0, 100):
                rejection_logger.warning(
                    "Rejected asset weight for %s | asset=%s | weight=%s",
                    symbol,
                    item.get("asset"),
                    item.get("weightPercentage"),
                )
                continue
            shares = parse_decimal(item.get("sharesNumber"))
            market_value = parse_decimal(item.get("marketValue"))
            shares_int = _to_int(shares) or 0
            market_value = market_value if market_value is not None else parse_decimal(0)
            validated.append(
                {
                    "symbol": symbol,
                    "id": symbol_id,
                    "asset": item.get("asset"),
                    "shares_number": shares_int,
                    "weight_percentage": weight,
                    "market_value": market_value,
                }
            )
        self.asset_repo.replace_snapshot(
            symbol_id,
            symbol,
            validated,
            ["symbol", "id", "asset", "shares_number", "weight_percentage", "market_value"],
        )

    def _update_holdings(self, symbol_id: int, symbol: str) -> None:
        data = self.client.get_json("etf/holdings", {"symbol": symbol})
        if not isinstance(data, dict) or "holdings" not in data:
            logger.warning("Unexpected holdings response for %s: %s", symbol, data)
            return
        holdings = data.get("holdings", [])
        cleaned = []
        for item in holdings:
            weight = parse_decimal(item.get("weightPercentage"))
            if not in_range(weight, 0, 100):
                rejection_logger.warning(
                    "Rejected holding weight for %s | asset=%s | weight=%s",
                    symbol,
                    item.get("asset"),
                    item.get("weightPercentage"),
                )
                continue
            shares = parse_decimal(item.get("sharesNumber"))
            market_value = parse_decimal(item.get("marketValue"))
            shares_int = _to_int(shares)
            market_value = market_value if market_value is not None else parse_decimal(0)
            identifier = item.get("isin") or item.get("securityCusip") or item.get("asset") or item.get("name")
            if not identifier:
                rejection_logger.warning("Holding without identifier for %s: %s", symbol, item)
                continue
            cleaned.append(
                {
                    "id": f"{symbol_id}:{identifier}",
                    "asset": item.get("asset"),
                    "name": item.get("name"),
                    "isin": item.get("isin"),
                    "security_cusip": item.get("securityCusip"),
                    "shares_number": shares_int,
                    "weight_percentage": weight,
                    "market_value": market_value,
                    "updated_at": item.get("updatedAt"),
                }
            )
        self.holdings_repo.replace_holdings(symbol_id, symbol, cleaned)

    def _update_market_risk(self) -> None:
        logger.info("Refreshing market risk premium data")
        try:
            data = self.client.get_json("market-risk-premium")
        except Exception:
            logger.exception("Failed to download market risk premium")
            return
        if not isinstance(data, list):
            logger.warning("Unexpected market risk premium payload: %s", data)
            return
        cleaned = []
        for item in data:
            cleaned.append(
                {
                    "country": item.get("country"),
                    "continent": item.get("continent"),
                    "countryRiskPremium": parse_decimal(item.get("countryRiskPremium")),
                    "totalEquityRiskPremium": parse_decimal(item.get("totalEquityRiskPremium")),
                }
            )
        self.risk_repo.upsert_country_risk(cleaned)

    def _update_industry_snapshot(self) -> None:
        logger.info("Refreshing industry performance snapshot")
        try:
            data = self.client.get_json("industry-performance-snapshot")
        except Exception:
            logger.exception("Failed to download industry performance snapshot")
            return
        if not isinstance(data, list):
            logger.warning("Unexpected industry performance payload: %s", data)
            return
        cleaned = []
        for item in data:
            cleaned.append({"industry": item.get("industry"), "average_change": parse_decimal(item.get("averageChange"))})
        self.risk_repo.upsert_industry_risk(cleaned)


__all__ = ["ETLPipeline"]
