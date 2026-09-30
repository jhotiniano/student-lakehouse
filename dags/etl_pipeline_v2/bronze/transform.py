import pandas as pd

def run(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.lower() for c in df.columns]
    for col in df.columns:
        if df[col].dtype == "float64":
            if ((df[col] % 1 == 0) | df[col].isnull()).all():
                df[col] = df[col].astype("Int64")
    for col in [c for c in df.columns if "datetime" in c.lower()]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df
