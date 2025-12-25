from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
import json
import logging
import pandas as pd

# Internal imports from your project
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import (
    enforce_schema,
    add_missing_flags,
    normalize_text,
    apply_mapping,
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)
from bootcamp_data.joins import safe_left_join

log = logging.getLogger(__name__)

@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path

def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    log.info(f"Reading inputs from {cfg.root}")
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users

def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    # 1. Fail-fast checks
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")

    assert_unique_key(users, "user_id")
    
    status_map = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    
    # 2. Clean Orders
    orders = (
        orders_raw.pipe(enforce_schema)
        .assign(status_clean=lambda d: apply_mapping(normalize_text(d["status"]), status_map))
        .pipe(add_missing_flags, cols=["amount", "quantity"])
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )
    
    # 3. Join Orders + Users
    analytics = safe_left_join(orders, users, on="user_id", validate="many_to_one")

    # 4. Final Enrichments
    # FIX: Use .assign() for winsorize because it works on a Series, not the whole DataFrame
    analytics = analytics.assign(
        amount=lambda df: winsorize(df["amount"])
    )

    # add_outlier_flag works on DataFrames, so .pipe() is correct here
    analytics = analytics.pipe(add_outlier_flag, col="amount")

    # 5. Return the result
    return analytics

def load_outputs(analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    """Write processed artifacts (idempotent)."""
    log.info(f"Writing analytics to {cfg.out_analytics}")
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)
    
    # Optional: write an orders-only table by dropping user-side columns
    user_side_cols = [c for c in users.columns if c != "user_id"]
    cols_to_drop = [c for c in user_side_cols if c in analytics.columns]
    orders_clean = analytics.drop(columns=cols_to_drop, errors="ignore")
    
    write_parquet(orders_clean, cfg.out_orders_clean)

def write_run_meta(cfg: ETLConfig, orders_raw: pd.DataFrame, users: pd.DataFrame, analytics: pd.DataFrame) -> None:
    # Note: Removed the '*' so positional arguments work in run_etl
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
    
    country_match_rate = (
        1.0 - float(analytics["country"].isna().mean())
        if "country" in analytics.columns
        else None
    )
    
    meta = {
        "rows_in_orders_raw": int(len(orders_raw)),
        "rows_in_users": int(len(users)),
        "rows_out_analytics": int(len(analytics)),
        "metrics": {
            "missing_created_at": missing_created_at,
            "country_match_rate": country_match_rate,
        },
        "config": asdict(cfg)
    }
    
    # Write the file (This was missing in your code)
    log.info(f"Writing run metadata to {cfg.run_meta}")
    with open(cfg.run_meta, "w") as f:
        json.dump(meta, f, indent=2, default=str)

def run_etl(cfg: ETLConfig) -> None:
    """
    Main entry point for the ETL pipeline.
    Orchestrates Extract -> Transform -> Load + Metadata.
    """
    log.info("Starting ETL run...")

    # 1. Extract
    log.info("Loading inputs...")
    orders_raw, users_raw = load_inputs(cfg)
    
    # 2. Transform
    log.info("Running transforms...")
    analytics = transform(orders_raw, users_raw)
    
    # 3. Load Outputs
    log.info("Writing outputs...")
    load_outputs(analytics, users_raw, cfg)
    
    # 4. Generate Metadata
    log.info("Generating run metadata...")
    write_run_meta(cfg, orders_raw, users_raw, analytics)
    
    log.info("ETL run complete!")