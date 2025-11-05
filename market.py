import requests
import polars as pl
from datetime import date
from database import DatabaseManager
import logging
from typing import List
import json


# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Marketdata(object):
    def __init__(self):
        # Пока что сделал все в одной базе данных, потом нужно подумать как лучше
        self.DBS = DatabaseManager(db_path='database.db')


    def get_current_info(self) -> bool:
        """
        Получение данных по выбранным бумагам
        :param secid: List[str], SECID бумаг
        :return: bool: Успешно ли собрана информация
        """

        # Все торгующиеся акции
        shares_url = 'https://iss.moex.com/iss/engines/stock/markets/shares/securities.json'

        response = requests.get(url=shares_url)
        if response.status_code != 200:
            logger.error("Не удалось подключиться к API для сбора информации по акциям")
            raise TimeoutError ("Не удалось подключиться к API Мосбиржи (акции)")

        api_data = response.json()

        # shares_data = {api_data["marketdata"]["columns"][0]: api_data["securities"]["data"][0][0]}
        #
        # print(len(api_data["securities"]["data"]))




        # Названия всех столбцов
        columns = [column for column in api_data["marketdata"]["columns"]]

        shares_data = {column : [] for column in columns}


        # Добавление информации по бумагам в словарь shares_data
        for i in range(len(api_data["securities"]["data"])):
            for j in range(len(api_data["securities"]["data"][i])):
                shares_data[columns[j]].append(api_data["securities"]["data"][i][j])

        print(shares_data)



t = Marketdata()
t.get_current_info()


