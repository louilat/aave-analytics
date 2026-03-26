import pandas as pd
from pandas import DataFrame
from src.utils.ethereum_addresses import to_checksum_address


def fetch_latest_aggregated_collateral_and_borrow(
    block_number: int, engine
) -> DataFrame:
    """Function to latest balances before block_number."""
    query = f"""
    WITH latest_balances AS (
        SELECT user_address, reserve, scaled_collateral, scaled_debt FROM (
            SELECT DISTINCT ON (user_address, reserve) *
            FROM balances
            WHERE block_number <= {block_number}
            ORDER BY user_address, reserve, block_number DESC
        ) latest
        WHERE scaled_collateral > 0 OR scaled_debt > 0
    )
    SELECT
        reserve,
        SUM(scaled_collateral) AS total_collateral,
        SUM(scaled_debt) AS total_debt
    FROM latest_balances
    GROUP BY reserve;
    """
    df = pd.read_sql(query, engine)
    return df


def preprocess_aggregated_reserves(aggregeted_reserves: DataFrame) -> DataFrame:
    aggregeted_reserves.reserve = aggregeted_reserves.reserve.apply(to_checksum_address)
    return aggregeted_reserves
