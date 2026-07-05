import pandas as pd


def clean_dataframe(df: pd.DataFrame):

    df = df.drop_duplicates()

    df = df.dropna()

    return df
