import logging
import pandas as pd
logger = logging.getLogger("etl_pipeline.silver.transform")
PAYMENT_TYPE_MAP = {1: "credit_card", 2: "cash", 3: "no_charge", 4: "dispute"}
RATECODE_MAP = {1: "standard", 2: "jfk", 3: "newark", 4: "nassau", 5: "negotiated", 6: "group_ride"}

def filter_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df[df["passenger_count"].notna() & (df["passenger_count"] > 0)]
    df = df[df["trip_distance"].notna() & (df["trip_distance"] > 0)]
    df = df[df["fare_amount"].notna() & (df["fare_amount"] >= 0)]
    df = df[df["tpep_pickup_datetime"].notna()]
    df = df[df["tpep_dropoff_datetime"].notna()]
    df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]
    logger.info(f"Filtro: {before:,} → {len(df):,} ({before-len(df):,} descartadas)")
    return df.reset_index(drop=True)

def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    duration = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60
    df["trip_duration_minutes"] = duration.round(2)
    df["speed_mph"] = (df["trip_distance"] / (duration / 60)).where(duration > 0)
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek
    df["is_weekend"] = df["pickup_day_of_week"].isin([5, 6])
    df["tip_percentage"] = ((df["tip_amount"] / df["fare_amount"]) * 100).where(df["fare_amount"] > 0).round(2)
    return df

def standardize_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    df["payment_type"] = df["payment_type"].map(PAYMENT_TYPE_MAP)
    df["ratecodeid"] = df["ratecodeid"].map(RATECODE_MAP)
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].map({"Y": True, "N": False})
    return df

def run(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.lower() for c in df.columns]
    df = filter_invalid_rows(df)
    df = add_derived_columns(df)
    df = standardize_categoricals(df)
    return df
