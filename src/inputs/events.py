import pandas as pd
from pandas import DataFrame


def fetch_borrows(from_block: int, to_block: int, engine) -> DataFrame:
    query = f"""
    SELECT 
        block_number,
        payload ->> 'OnBehalfOf' AS user,
        payload ->> 'Reserve' AS reserve,
        payload->> 'Amount' AS amount
    FROM borrow_events
    WHERE block_number >= {from_block}
    AND block_number <= {to_block};
    """
    borrows = pd.read_sql(query, engine)
    return borrows


def fetch_supplies(from_block: int, to_block: int, engine) -> DataFrame:
    query = f"""
    SELECT 
        block_number,
        payload ->> 'OnBehalfOf' AS user,
        payload ->> 'Reserve' AS reserve,
        payload->> 'Amount' AS amount
    FROM supply_events
    WHERE block_number >= {from_block}
    AND block_number <= {to_block};
    """
    supplies = pd.read_sql(query, engine)
    return supplies


def fetch_withdraws(from_block: int, to_block: int, engine) -> DataFrame:
    query = f"""
    SELECT 
        block_number,
        payload ->> 'User' AS user,
        payload ->> 'Reserve' AS reserve,
        payload->> 'Amount' AS amount
    FROM withdraw_events
    WHERE block_number >= {from_block}
    AND block_number <= {to_block};
    """
    withdraws = pd.read_sql(query, engine)
    return withdraws


def fetch_repays(from_block: int, to_block: int, engine) -> DataFrame:
    query = f"""
    SELECT 
        block_number,
        payload ->> 'User' AS user,
        payload ->> 'Reserve' AS reserve,
        payload->> 'Amount' AS amount
    FROM repay_events
    WHERE block_number >= {from_block}
    AND block_number <= {to_block};
    """
    repays = pd.read_sql(query, engine)
    return repays


def fetch_liquidations(from_block: int, to_block: int, engine) -> DataFrame:
    query = f"""
    SELECT 
        block_number,
        payload ->> 'User' AS user,
        payload ->> 'Liquidator' as liquidator,
        payload ->> 'DebtAsset' AS debt_reserve,
        payload ->> 'CollateralAsset' AS collateral_reserve,
        payload ->> 'DebtToLiquidate' AS liquidated_debt,
        payload ->> 'SeizedCollateral' AS seized_collateral
    FROM repay_events
    WHERE block_number >= {from_block}
    AND block_number <= {to_block};
    """
    repays = pd.read_sql(query, engine)
    return repays
