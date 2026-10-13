import logging
import pandas as pd


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.lower() for c in df.columns]
    return df


def cast_integer_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == "float64":
            if ((df[col] % 1 == 0) | df[col].isnull()).all():
                df[col] = df[col].astype("Int64")
                logging.getLogger("etl_pipeline").debug(f"Columna '{col}' convertida float64 -> Int64")
    return df


def cast_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    datetime_cols = [c for c in df.columns if "datetime" in c.lower()]
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        logging.getLogger("etl_pipeline").debug(f"Columna '{col}' parseada como datetime")
    return df


def run(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_column_names(df)
    df = cast_integer_columns(df)
    df = cast_datetime_columns(df)
    return df