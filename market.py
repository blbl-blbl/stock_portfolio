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
        :return: bool: Успешно ли собрана информация
        """

        # Все торгующиеся акции
        shares_url = 'https://iss.moex.com/iss/engines/stock/markets/shares/securities.json'

        response = requests.get(url=shares_url)
        if response.status_code != 200:
            logger.error("Не удалось подключиться к API для сбора информации по акциям")
            raise TimeoutError ("Не удалось подключиться к API Мосбиржи (акции)")

        api_data = response.json()
        logger.info("Установлено подключение к API Мосбиржи для акций")

        # Названия всех столбцов
        columns = [column for column in api_data["marketdata"]["columns"]]

        # Заполняем None все значения создаваемого словаря
        shares_data = {column : [None for _ in range(len(api_data["marketdata"]["data"]))] for column in columns}



        # Добавление информации по бумагам в словарь shares_data
        for i in range(len(api_data["marketdata"]["data"])):
            for j in range(len(api_data["marketdata"]["data"][i])):
                shares_data[columns[j]][i] = api_data["marketdata"]["data"][i][j]

        # Создание DateFrame Polars
        df_shares = pl.DataFrame(data=shares_data, nan_to_null=True, strict=False)

        # Удаляем столбцы, где все значения null
        df_shares = df_shares[[s.name for s in df_shares if not (s.null_count() == df_shares.height)]]

        # Сохранение в SQL
        self.DBS.add_dataframe_to_table(df=df_shares,
                                        table_name='current_marketdata_info',
                                        if_exists='replace')




t = Marketdata()
t.get_current_info()


