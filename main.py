from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from src.utils.logger import get_logger
from src.inputs.events import (
    fetch_borrows,
    fetch_repays,
    fetch_supplies,
    fetch_withdraws,
)

logger = get_logger()
logger.info("Starting job")

load_dotenv()

POSTGRES_PWD = os.getenv("POSTGRES_PWD")
encoded_pwd = quote_plus(POSTGRES_PWD)

START_BLOCK = 21525893  # 2025-01-01
END_BLOCK = 24136053  # 2025-01-01

engine = create_engine(
    f"postgresql://reader:{encoded_pwd}@localhost:5432/aavev3-indexer"
)

logger.info("--> Collecting Borrow events...")
borrow = fetch_borrows(from_block=START_BLOCK, to_block=END_BLOCK, engine=engine)

logger.info("--> Collecting Repay events...")
repay = fetch_repays(from_block=START_BLOCK, to_block=END_BLOCK, engine=engine)

logger.info("--> Collecting Supply events...")
supply = fetch_supplies(from_block=START_BLOCK, to_block=END_BLOCK, engine=engine)

logger.info("--> Collecting Withdraw events...")
withdraw = fetch_withdraws(from_block=START_BLOCK, to_block=END_BLOCK, engine=engine)


logger.info("Done!")
