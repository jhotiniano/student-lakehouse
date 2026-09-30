import logging
import pandas as pd
logger = logging.getLogger("etl_pipeline.gold.transform")

def enrich_with_zones(df: pd.DataFrame, df_zones: pd.DataFrame) -> pd.DataFrame:
    zones_pickup = df_zones.rename(columns={"LocationID": "pulocationid", "Zone": "pickup_zone", "Borough": "pickup_borough"})[["pulocationid", "pickup_zone", "pickup_borough"]]
    zones_dropoff = df_zones.rename(columns={"LocationID": "dolocationid", "Zone": "dropoff_zone", "Borough": "dropoff_borough"})[["dolocationid", "dropoff_zone", "dropoff_borough"]]
    df = df.merge(zones_pickup, on="pulocationid", how="left")
    df = df.merge(zones_dropoff, on="dolocationid", how="left")
    return df

def build_hourly_demand(df: pd.DataFrame) -> pd.DataFrame:
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date
    return df.groupby(["pickup_date", "pickup_hour", "is_weekend"]).agg(total_trips=("vendorid", "count"), total_passengers=("passenger_count", "sum"), avg_trip_distance=("trip_distance", "mean"), avg_fare=("fare_amount", "mean"), avg_duration_minutes=("trip_duration_minutes", "mean")).round(2).reset_index()

def build_zone_performance(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["pickup_zone", "pickup_borough"]).agg(total_trips=("vendorid", "count"), total_revenue=("total_amount", "sum"), avg_fare=("fare_amount", "mean"), avg_tip_percentage=("tip_percentage", "mean"), avg_trip_distance=("trip_distance", "mean")).round(2).reset_index()

def build_tip_analysis(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(["payment_type", "pickup_hour", "is_weekend"]).agg(total_trips=("vendorid", "count"), avg_tip_amount=("tip_amount", "mean"), avg_tip_percentage=("tip_percentage", "mean")).round(2).reset_index()

def build_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date
    return df.groupby("pickup_date").agg(total_trips=("vendorid", "count"), total_revenue=("total_amount", "sum"), total_passengers=("passenger_count", "sum"), avg_speed_mph=("speed_mph", "mean"), avg_trip_duration_minutes=("trip_duration_minutes", "mean"), avg_fare=("fare_amount", "mean"), avg_tip_percentage=("tip_percentage", "mean")).round(2).reset_index()

def run(df_silver: pd.DataFrame, df_zones: pd.DataFrame) -> dict:
    df_enriched = enrich_with_zones(df_silver, df_zones)
    return {"hourly_demand": build_hourly_demand(df_enriched), "zone_performance": build_zone_performance(df_enriched), "tip_analysis": build_tip_analysis(df_enriched), "daily_summary": build_daily_summary(df_enriched)}
