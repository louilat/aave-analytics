import pandas as pd
from pandas import DataFrame


def fetch_last_block_timestamp(engine) -> DataFrame:
    """Fetch the timestamp of the latest block from the database."""
    query = """
    SELECT block_number, timestamp 
    FROM blocks
    ORDER BY block_number DESC
    LIMIT 1;
    """
    df = pd.read_sql(query, engine)
    return df


def fetch_latest_indexes(
    reserves_list: list[str], block_number: int, conn
) -> DataFrame:
    try:
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
        df = pd.read_sql(query, conn)

    except Exception as e:
        print("Error:", e)
    finally:
        conn.close()
    return df
