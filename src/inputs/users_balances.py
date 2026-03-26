import pandas as pd
from pandas import DataFrame
from src.utils.ethereum_addresses import to_checksum_address


def fetch_latest_balances(block_number: int, engine) -> DataFrame:
    """Function to latest balances before block_number."""

    query = f"""
        SELECT block_number AS last_update, user_address, reserve, scaled_collateral, scaled_debt FROM (
            SELECT DISTINCT ON (user_address, reserve) *
            FROM balances
            WHERE block_number <= {block_number}
            ORDER BY user_address, reserve, block_number DESC
        ) latest
        WHERE scaled_collateral > 0 OR scaled_debt > 0;
    """
    balances = pd.read_sql(query, engine)
    return balances


def fetch_emodes(block_number: int, engine) -> DataFrame:
    query = f"""
    SELECT DISTINCT ON (payload->>'User')
    payload->>'User' AS user_address,
    (payload->>'CategoryId')::numeric AS emode
    FROM useremode_set_events
    WHERE block_number <= {block_number}
    ORDER BY payload->>'User', block_number DESC;
    """
    emodes = pd.read_sql(query, engine)
    return emodes


def preprocess_users_balances(users_balances: DataFrame) -> DataFrame:
    users_balances.user_address = users_balances.user_address.apply(to_checksum_address)
    users_balances.reserve = users_balances.reserve.apply(to_checksum_address)
    users_balances.scaled_collateral = users_balances.scaled_collateral.apply(int)
    users_balances.scaled_debt = users_balances.scaled_debt.apply(int)
    return users_balances
