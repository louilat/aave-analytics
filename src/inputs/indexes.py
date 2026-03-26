import pandas as pd
from pandas import DataFrame


def fetch_latest_indexes(
    reserves_list: list[str], block_number: int, engine
) -> DataFrame:
    values = ",                ".join(f"('{r}')" for r in reserves_list)

    query = f"""
        SELECT r.reserve,
            e.block_number,
            e.payload->>'LiquidityIndex'       AS liquidity_index,
            e.payload->>'VariableBorrowIndex'  AS borrow_index
        FROM (VALUES
            {values}
        ) AS r(reserve)
        CROSS JOIN LATERAL (
            SELECT block_number, payload
            FROM reservedataupdated_events
            WHERE payload->>'Reserve' = r.reserve
              AND block_number <= {block_number}
            ORDER BY block_number DESC
            LIMIT 1
        ) e;
    """
    indexes = pd.read_sql(query, engine)
    return indexes


def preprocess_indexes(indexes: DataFrame) -> DataFrame:
    indexes.liquidity_index = indexes.liquidity_index.apply(int) * 1e-27
    indexes.borrow_index = indexes.borrow_index.apply(int) * 1e-27
    return indexes
