import requests
import polars as pl
from database import DatabaseManager
import logging
import config


# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Marketdata(object):
    def __init__(self):
        # Пока что сделал все в одной базе данных, потом нужно подумать как лучше
        self.DBS = DatabaseManager(db_path='database.db')
        self.shares_url = config.shares_url
        self.bonds_url = config.bonds_url
        self.BOARDID_SHARES = config.BOARDID_SHARES
        self.BOARDID_ETFS = config.BOARDID_ETFS
        self.BOARDID_BONDS = config.BOARDID_BONDS
        self.DEFAULT_BONDS = config.DEFAULT_BONDS


    def get_current_info_shares_and_etfs(self) -> bool:
        """
        Получение данных по акциям и ETF
        :return: bool: Успешно ли собрана информация
        """

        response = requests.get(url=self.shares_url)
        if response.status_code != 200:
            logger.error("Не удалось подключиться к API для сбора информации по акциям и ETF")
            return False

        api_data = response.json()
        logger.info("Установлено подключение к API Мосбиржи для акций и ETF")

        try:
            # Названия всех столбцов
            columns = [column for column in api_data["marketdata"]["columns"]]

            # Заполняем None все значения создаваемого словаря
            data = {column : [None for _ in range(len(api_data["marketdata"]["data"]))] for column in columns}

            # Добавление информации по бумагам в словарь
            for i in range(len(api_data["marketdata"]["data"])):
                for j in range(len(api_data["marketdata"]["data"][i])):
                    data[columns[j]][i] = api_data["marketdata"]["data"][i][j]
        except Exception as e:
            logger.error(f"Возникла ошибка при сборе информации по акциям и ETF \n{e}")
            return False

        try:
            # Создание DateFrame Polars
            df_shares = pl.DataFrame(data=data, nan_to_null=True, strict=False)

            # Удаляем столбцы, где все значения null
            df_shares = df_shares[[s.name for s in df_shares if not (s.null_count() == df_shares.height)]]

            # Оставляем только бумаги у которых нужный режим торгов
            df_shares = df_shares.filter(pl.col("BOARDID").is_in(self.BOARDID_SHARES))

            # Добавляем столбец с типом бумаг
            df_shares = df_shares.with_columns(pl.lit('share').alias('securities_type'))

            # Сохранение в SQL
            self.DBS.add_dataframe_to_table(df=df_shares,
                                            table_name='current_marketdata_shares',
                                            if_exists='replace')
        except Exception as e:
            logger.error(f"Возникла ошибка при сохранении информации по акциям \n{e}")
            return False

        logger.info("Сбор последней информации по акциям прошел успешно")

        try:
            # Создание DateFrame Polars
            df_etfs = pl.DataFrame(data=data, nan_to_null=True, strict=False)

            # Удаляем столбцы, где все значения null
            df_etfs = df_etfs[[s.name for s in df_etfs if not (s.null_count() == df_etfs.height)]]

            # Оставляем только бумаги у которых нужный режим торгов
            df_etfs = df_etfs.filter(pl.col("BOARDID").is_in(self.BOARDID_ETFS))

            # Добавляем столбец с типом бумаг
            df_etfs = df_etfs.with_columns(pl.lit('ETF').alias('securities_type'))

            # Сохранение в SQL
            self.DBS.add_dataframe_to_table(df=df_etfs,
                                            table_name='current_marketdata_etfs',
                                            if_exists='replace')
        except Exception as e:
            logger.error(f"Возникла ошибка при сохранении информации по ETF \n{e}")
            return False

        logger.info("Сбор последней информации по ETF прошел успешно")
        return True

    def get_current_info_bonds(self) -> bool:
        """
        Получение данных по облигациям
        :param self:
        :return:
        """

        response = requests.get(url=self.bonds_url)
        if response.status_code != 200:
            logger.error("Не удалось подключиться к API для сбора информации по облигациям")
            return False

        api_data = response.json()
        logger.info("Установлено подключение к API Мосбиржи для облигаций")

        # Сбор из блока securities
        try:
            # Названия всех столбцов
            columns = [column for column in api_data["securities"]["columns"]]

            # Заполняем None все значения создаваемого словаря
            data = {column: [None for _ in range(len(api_data["securities"]["data"]))] for column in columns}

            # Добавление информации по бумагам в словарь shares_data
            for i in range(len(api_data["securities"]["data"])):
                for j in range(len(api_data["securities"]["data"][i])):
                    data[columns[j]][i] = api_data["securities"]["data"][i][j]
        except Exception as e:
            logger.error(f"Возникла ошибка при сборе информации по облигациям (БЛОК securities) \n{e}")
            return False

        try:
            # Создание DateFrame Polars
            df_bonds_s = pl.DataFrame(data=data, nan_to_null=True, strict=False)

            # Удаляем столбцы, где все значения null
            df_bonds_s = df_bonds_s[[s.name for s in df_bonds_s if not (s.null_count() == df_bonds_s.height)]]

            # Оставляем только бумаги у которых нужный режим торгов
            df_bonds_s = df_bonds_s.filter(pl.col("BOARDID").is_in(self.BOARDID_BONDS))

            # Добавляем столбец с типом бумаг
            df_bonds_s = df_bonds_s.with_columns(pl.lit('bond').alias('securities_type'))

            # Добавляем столбец-индикатор 'Дефолт'
            df_bonds_s = df_bonds_s.with_columns(
                pl.when(pl.col("BOARDID").is_in(self.DEFAULT_BONDS))
                .then(1)
                .otherwise(0)
                .alias('is_default')
            )

        except Exception as e:
            logger.error(f"Возникла ошибка при сохранении информации по облигациям (БЛОК securities) \n{e}")
            return False


        try:
            # Названия всех столбцов
            columns = [column for column in api_data["marketdata"]["columns"]]

            # Заполняем None все значения создаваемого словаря
            data = {column: [None for _ in range(len(api_data["marketdata"]["data"]))] for column in columns}

            # Добавление информации по бумагам в словарь shares_data
            for i in range(len(api_data["marketdata"]["data"])):
                for j in range(len(api_data["marketdata"]["data"][i])):
                    data[columns[j]][i] = api_data["marketdata"]["data"][i][j]
        except Exception as e:
            logger.error(f"Возникла ошибка при сборе информации по облигациям (БЛОК marketdata) \n{e}")
            return False

        try:
            # Создание DateFrame Polars
            df_bonds_m = pl.DataFrame(data=data, nan_to_null=True, strict=False)

            # Удаляем столбцы, где все значения null
            df_bonds_m = df_bonds_m[[s.name for s in df_bonds_m if not (s.null_count() == df_bonds_m.height)]]

        except Exception as e:
            logger.error(f"Возникла ошибка при сохранении информации по облигациям (БЛОК marketdata) \n{e}")
            return False

        try:
            full_df = df_bonds_s.join(df_bonds_m, on='SECID', how='inner', suffix='_m')
        except Exception as e:
            logger.error(f"Возникла ошибка при объединении датафреймов с информацией по облигациям\n{e}")
            return False

        try:
            # Сохранение в SQL
            self.DBS.add_dataframe_to_table(df=full_df,
                                            table_name='current_marketdata_bonds',
                                            if_exists='replace')
        except Exception as e:
            logger.error(f"Возникла ошибка при сохранении информации по облигациям в базу данных \n{e}")
            return False

        logger.info("Сбор последней информации по облигациям прошел успешно")

        return True

t = Marketdata()
t.get_current_info_shares_and_etfs()
t.get_current_info_bonds()

