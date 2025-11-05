import requests
import polars as pl
from datetime import date
from database import DatabaseManager
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Marketdata(object):
    def __init__(self):
        # Пока что сделал все в одной базе данных, потом нужно подумать как лучше
        self.DBS = DatabaseManager(db_path='database.db')



