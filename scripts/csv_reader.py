import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


def read_csv(csv_path):
    """
    Read a CSV file and return a Pandas DataFrame.
    """

    logger.info(f"Reading file: {csv_path}")

    dataframe = pd.read_csv(csv_path)

    logger.info(f"Rows Loaded: {len(dataframe)}")

    return dataframe