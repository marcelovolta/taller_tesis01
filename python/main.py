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
    
    # Get settings from env and yaml
    TMDB_API_READ_ACCESS_TOKEN = config.TMDB_API_READ_ACCESS_TOKEN
    TMDB_API_KEY = config.TMDB_API_KEY
    DB_NAME = config.DB_NAME
    DB_SCHEMA = config.DB_SCHEMA
    POSTGRE_USER = config.POSTGRE_USER
    POSTGRE_PASS = config.POSTGRE_PASS
    YOUTUBE_API_KEY = config.YOUTUBE_API_KEY
    PROJECT_NAME = config.PROJECT_NAME
    YEARS = config.YEARS
    
    
    # Get movies from TMDB
    df_movies = get_tmdb_movies_us(max_pages=None)
    # print(df_movies.head())
    print(f"Total películas recuperadas: {len(df_movies)}")
    load_to_postgres(df_movies)
    print("Tabla public.movies_2025 cargada correctamente.")


    # Find trailers
    # Set reset to True to clear the progress and comments tables
    # and reprocess everything from scratch.
    # process_trailer_comments(reset=False)

    # Scrape opening-weekend box office and production budget from The Numbers.
    # Set reset to True to clear box_office and box_office_progress tables.
    # process_box_office(reset=False)

'''
This is the main function that will be called when the script is run.
Do not allow it to be called from outside this file.
'''
if __name__ == "__main__":
    main()