import pandas as pd
from pandas import DataFrame

from src.utils.ethereum_addresses import to_checksum_address


def fetch_historical_prices(from_block: int, to_block: int, engine) -> DataFrame:
    """Function to fetch prices."""
    try:
        query = f"""
        SELECT *
        FROM prices
        WHERE block_number >= {from_block}
        AND block_number <= {to_block}
        """
        df = pd.read_sql(query, engine)
        return df if not df.empty else None

    except Exception as e:
        print("Error fetching liquidations:", e)
        return None


def preprocess_prices(prices: DataFrame) -> DataFrame:
    prices.token = prices.token.apply(to_checksum_address)
    prices = prices.rename(columns={"token": "reserve"})
    prices.price = prices.price * 1e-8
    return prices
