# Core Python
import logging
from operator import ge
import os
import datetime as dt 

# External libraries
import pandas as pd
import polars as pl
import duckdb

# Modular code
from src import * 

# Log setup
os.makedirs('logs', exist_ok=True)
this_date = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
log_name = f'logs/log_{this_date}.log'
logging.basicConfig(level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(name)s - %(lineno)d - %(message)s', \
    datefmt='%Y-%m-%d %H:%M:%S', 
    handlers = [logging.FileHandler(log_name, mode='w', encoding='utf-8'), \
    logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():

    # Start logging
    logger.info("Start run")

    # Find trailers
    trailers = find_trailers("werewolf game")
    logger.info(f"Found {len(trailers)} trailers")

    # Get comments for each trailer
    comments_df = pd.DataFrame()
    for trailer in trailers:
        comments = get_comments(trailer["video_id"], limit_date=dt.date(2025, 1, 21))
        logger.info(f"Found {len(comments)} comments for {trailer['title']}")
        comments_df = pd.concat([comments_df, comments])

    # Save comments to a CSV file
    comments_df.to_csv(f"../data/comments_{this_date}.csv", index=False)

'''
This is the main function that will be called when the script is run.
Do not allow it to be called from outside this file.
'''
if __name__ == "__main__":
    main()