import pandas as pd

REQUIRED_COLUMNS = [
    "vehicle_id",
    "brand",
    "model",
    "year",
    "fuel_type",
]


def validate_dataframe(df: pd.DataFrame):

    missing = []

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            missing.append(col)

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )

    return True