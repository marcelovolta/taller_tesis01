import yaml
import logging
import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

# from python.src.movie_database import TABLE_NAME


print(os.getcwd())
config_path = "./config.yaml"
logger = logging.getLogger(__name__)

class SecretSettings(BaseSettings):
    '''
    Use to set secret settings: Passwords, API keys and the like
    Non-sensitive settings go into the YAML file
    Declare one property per secret setting to get from the .env file
    '''
    tmdb_api_read_access_token: str = '' #Field(alias='my_api_key')
    tmdb_api_key: str = ''
    postgre_database: str = ''
    postgre_schema: str = ''
    postgre_user: str = ''
    postgre_pass: str = ''
    youtube_api_key: str = ''
    

    class Config:
        env_file = "./.env"


secret_settings = SecretSettings()
TMDB_API_READ_ACCESS_TOKEN = secret_settings.tmdb_api_read_access_token
TMDB_API_KEY = secret_settings.tmdb_api_key
POSTGRE_USER = secret_settings.postgre_user
POSTGRE_PASS = secret_settings.postgre_pass
YOUTUBE_API_KEY = secret_settings.youtube_api_key


try:
    with open(config_path, "r") as f:
        
        _cfgGeneral = yaml.safe_load(f)
        PROJECT_NAME = _cfgGeneral.get("PROJECT_NAME")
        YEARS = _cfgGeneral.get("YEARS")
        POSTGRE_HOST = _cfgGeneral.get("POSTGRE_HOST")
        POSTGRE_PORT = _cfgGeneral.get("POSTGRE_PORT")
        DB_NAME = _cfgGeneral.get("DB_NAME")
        DB_SCHEMA = _cfgGeneral.get("DB_SCHEMA")
        TABLE_NAME = _cfgGeneral.get("TABLE_NAME")
        START_DATE = _cfgGeneral.get("START_DATE")
        END_DATE = _cfgGeneral.get("END_DATE")

except Exception as e:
    logger.error(f"Exception while loading the yaml config file: {e}")
