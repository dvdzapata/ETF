"""Entry point for the ETF data ingestion pipeline."""
from __future__ import annotations

import logging

from etf_pipeline.config import build_settings
from etf_pipeline.db import Database
from etf_pipeline.http_client import FMPClient
from etf_pipeline.logging_utils import setup_logging
from etf_pipeline.pipeline import ETLPipeline
from etf_pipeline.repositories import (
    HoldingsRepository,
    RiskRepositories,
    SnapshotRepository,
    SymbolRepository,
    TimeSeriesRepository,
)
from etf_pipeline.state import DownloadState


def main() -> None:
    settings = build_settings()
    setup_logging(settings.log_dir)
    logger = logging.getLogger(__name__)
    logger.info("Initialising ETF pipeline")

    state = DownloadState(settings.state_file)
    database = Database(settings.database, max_pool_size=max(settings.max_workers, 4))
    database.connect()

    symbol_repo = SymbolRepository(database)
    db_time_series = {
        "etf_cotizacion": TimeSeriesRepository(database, "etf_cotizacion"),
        "etf_ohlc_ajustado": TimeSeriesRepository(database, "etf_ohlc_ajustado"),
        "etf_por_horas": TimeSeriesRepository(database, "etf_por_horas"),
        "etf_dividendos": TimeSeriesRepository(database, "etf_dividendos"),
    }
    country_repo = SnapshotRepository(database, "etf_exposicion_pais", ["id", "country"])
    industry_repo = SnapshotRepository(database, "etf_exposicion_industria", ["id", "industry"])
    asset_repo = SnapshotRepository(database, "etf_exposicion_assets", ["id", "asset"])
    holdings_repo = HoldingsRepository(database)
    risk_repo = RiskRepositories(database)

    with FMPClient(settings.api, timeout=settings.request_timeout, retries=settings.request_retries) as client:
        pipeline = ETLPipeline(
            settings=settings,
            client=client,
            symbol_repo=symbol_repo,
            db_time_series=db_time_series,
            country_repo=country_repo,
            industry_repo=industry_repo,
            asset_repo=asset_repo,
            holdings_repo=holdings_repo,
            risk_repo=risk_repo,
            state=state,
        )
        pipeline.run()

    database.close()


if __name__ == "__main__":
    main()
