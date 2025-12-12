import requests
import polars as pl
from database import DatabaseManager
import logging
import config
from datetime import datetime, date
from tqdm import tqdm
import pandas as pd


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
        self.currencies_url = config.currencies_url
        self.urls_settings = config.urls_settings


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


    def translate_to_rub(self):
        """
        Добавление в SQL столбца с валютой для каждой облигации
        :return:
        """

        currency_df = self.DBS.read_table_to_dataframe(
            table_name='current_marketdata_currency',
            columns=['SECID', 'LASTVALUE']
        )
        currency_df = currency_df.rename({'LASTVALUE' : 'CURRENCY'})


        bonds_df = self.DBS.read_table_to_dataframe(
            table_name='current_marketdata_bonds'
        )

        # Соединение
        try:
            merged = bonds_df.join(
                other=currency_df,
                left_on='FACEUNIT',
                right_on='SECID',
                how='left'
            )
        except Exception as e:
            logger.error('Ошибка при добавлении столбца с курсом валют в DataFrame')
            raise e

        self.DBS.add_dataframe_to_table(df=merged,
                                        table_name='current_marketdata_bonds',
                                        if_exists='replace')

        logger.info('Курсы валют успешно добавлены в базу данных')

    @staticmethod
    def get_conn(url:str, try_count:int=5):
        """
        Установление подключения

        :param url: str: url-адресс для подлкючения
        :param try_count: int: количество попыток подключения
        :return: str: json формат страницы
        """

        for i in range(try_count):
            try:
                response = requests.get(url)
                data = response.json()
                return data
            except:
                continue
        logger.error(f"Не удалось подключиться к API мосбиржи по ссылке {url}")
        return False

    @staticmethod
    def str_to_datetime(date_string: str, format_code: str):
        """ Преобразует строку в datetime формат

        :param date_string: str: строка даты
        :param format_code: str: формат в котором подана строка. Например "%Y-%m-%d %H:%M:%S"
        """

        datetime_object = datetime.strptime(date_string, format_code)
        date_object = datetime_object.date()
        return date_object

    @staticmethod
    def marketdata_proccesing(data, first_ind:int, second_ind:int):
        """
        Извлечение данных о цене и дате из json

        :param data: json-формат
        :param first_ind: int: индекс столбца с ценой
        :param second_ind: int: индекс столбца с датой
        :return:
        """

        # Словарь в котором secid : название валюты
        currencies = {}

        if data:
            for i in range(len(data)):
                val1 = data[i][first_ind]
                val2 = data[i][second_ind]

                currencies[val1] = val2

            return currencies
        return False


    def get_price_history(self, active_type:str, operation:str,
                         start_date:date = date(year=2000, month=1, day=1),
                         end_year = datetime.now().year):
        """
        Парсинг валютных курсов

        :param active_type: str: тип актива ('currency', 'shares', 'bonds', 'index')
        :param start_date: int: год начала сбора данных (по умолчанию 2000 год)
        :param end_year: int: год окончания сбора данных + 1 (по умолчанию текущий год + 1)
        :param operation: str: тип операции - замена ('replace') или добавление ('append')
        :return:
        """

        try:
            start_year = start_date.year
            start_month = start_date.month
            start_day = start_date.day

            engine = self.urls_settings[active_type][0]
            market = self.urls_settings[active_type][1]
            table_name = self.urls_settings[active_type][2]


            data = self.get_conn(self.currencies_url)
            if not data:
                print("Не удалось подключиться к API Мосбиржи")
                return False

            cur_data = data['securities']['data']
            currencies = self.marketdata_proccesing(data=cur_data, first_ind=1, second_ind=4)
            currencies_secids = list(currencies.keys())

            full_df = pd.DataFrame()

            for secid in tqdm(currencies_secids):
                logger.info(f"Начат сбор данных по {secid}")
                secid_df = pd.DataFrame()

                for year in range(start_year, end_year+1):

                    # 1 год парсим с определенной даты, а все последующие годы с 1 января
                    if year == start_year:
                        date_prices_json = self.get_conn(
                            url=f'https://iss.moex.com/iss/engines/{engine}/markets/{market}/securities/{secid}/candles.json?from={year}-{start_month}-{start_day}&till={year}-12-31&interval=24'
                        )
                    else:
                        date_prices_json = self.get_conn(
                            url=f'https://iss.moex.com/iss/engines/{engine}/markets/{market}/securities/{secid}/candles.json?from={year}-01-01&till={year}-12-31&interval=24'
                        )

                    date_prices_json = date_prices_json['candles']['data']
                    date_prices_dict_old = self.marketdata_proccesing(data=date_prices_json, first_ind=7, second_ind=1)

                    if not date_prices_dict_old:
                        continue

                    date_prices_dict_new = {}

                    # Каждый ключ преобразуем из формата str "%Y-%m-%d %H:%M:%S" в datetime "%Y-%m-%d"
                    for key, value in date_prices_dict_old.items():
                        new_key = self.str_to_datetime(key,
                                                       format_code="%Y-%m-%d %H:%M:%S")
                        date_prices_dict_new[new_key] = value

                    df = pd.DataFrame(list(date_prices_dict_new.items()), columns=['date', secid])

                    # Присоединение данных за год
                    secid_df = pd.concat([secid_df, df], ignore_index=True)
                    logger.info(f"Собраны данные по {secid} за год {year}")

                # Объединение данных по валюте в датафрейм
                if not secid_df.empty:
                    if full_df.empty:
                        full_df = secid_df
                    else:
                        full_df = pd.merge(full_df, secid_df, on='date', how='outer')

            # Перевод в датафрейм поларс
            polars_dataframe = pl.from_pandas(full_df)

            # Сохранение в SQL
            self.DBS.add_dataframe_to_table(df=polars_dataframe,
                                            table_name=table_name,
                                            if_exists=operation)

        except Exception as Ex:
            logger.error(f"Возникла ошибка {Ex}")
            raise Ex

t = Marketdata()
# t.get_current_info_shares_and_etfs()
# t.get_current_info_bonds()
# t.get_currencies()
# t.translate_to_rub()
t.get_price_history(operation='replace', active_type='currency', start_date=date(2025, 1,1))
