import requests
import polars as pl
from datetime import date
from database import DatabaseManager
import logging
from typing import List
import config


# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Marketdata(object):
    def __init__(self):
        # Пока что сделал все в одной базе данных, потом нужно подумать как лучше
        self.DBS = DatabaseManager(db_path='database.db')
        self.shares_url = config.shares_url
        self.BOARDID_SHARES = config.BOARDID_SHARES

    # Нужно добавить сбор инфы по облигациям
    # Возможно нужно разделить это на несколько функций
    # Типо 1 функция для сбора данных по акциям, 2 функция для сбора по облигациям
    # Непонятно нужно ли все хранить вместе или по отедльности
    def get_current_info_shares(self) -> bool:
        """
        Получение данных по выбранным бумагам
        :return: bool: Успешно ли собрана информация
        """

        response = requests.get(url=self.shares_url)
        if response.status_code != 200:
            logger.error("Не удалось подключиться к API для сбора информации по акциям")
            return False

        api_data = response.json()
        logger.info("Установлено подключение к API Мосбиржи для акций")

        try:
            # Названия всех столбцов
            columns = [column for column in api_data["marketdata"]["columns"]]

            # Заполняем None все значения создаваемого словаря
            shares_data = {column : [None for _ in range(len(api_data["marketdata"]["data"]))] for column in columns}

            # Добавление информации по бумагам в словарь shares_data
            for i in range(len(api_data["marketdata"]["data"])):
                for j in range(len(api_data["marketdata"]["data"][i])):
                    shares_data[columns[j]][i] = api_data["marketdata"]["data"][i][j]
        except Exception as e:
            logger.error(f"Возникла ошибка при сборе информации по акциям \n{e}")
            return False

        try:
            # Создание DateFrame Polars
            df_shares = pl.DataFrame(data=shares_data, nan_to_null=True, strict=False)

            # Удаляем столбцы, где все значения null
            df_shares = df_shares[[s.name for s in df_shares if not (s.null_count() == df_shares.height)]]

            # Оставляем только бумаги у которых Режим торгов TQBR
            df_shares = df_shares.filter(pl.col("BOARDID").is_in(self.BOARDID_SHARES))

            # Добавляем столбец с типом бумаг
            df_shares = df_shares.with_columns(pl.lit('Акция').alias('securities_type'))

            # Сохранение в SQL
            self.DBS.add_dataframe_to_table(df=df_shares,
                                            table_name='current_marketdata_shares',
                                            if_exists='replace')
        except Exception as e:
            logger.error(f"Возникла ошибка при сохранении информации по акциям \n{e}")
            return False

        logger.info("Сбор последней информации по акциям прошел успешно")
        return True

def get_current_info_bonds(self) -> bool:
    pass


t = Marketdata()
t.get_current_info_shares()


