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
        self.urls_settings = config.urls_settings
        self.split_url = config.split_url
        self.rename_url = config.rename_url


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
            first_ind = self.urls_settings[active_type][3]
            second_ind = self.urls_settings[active_type][4]
            active_url = self.urls_settings[active_type][5]

            data = self.get_conn(active_url)
            if not data:
                print("Не удалось подключиться к API Мосбиржи")
                return False

            cur_data = data['securities']['data']
            currencies = self.marketdata_proccesing(data=cur_data, first_ind=first_ind, second_ind=second_ind)
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

            full_df.columns = full_df.columns.str.replace('-', '_')

            # Перевод в датафрейм поларс
            polars_dataframe = pl.from_pandas(full_df)

            # Сохранение в SQL
            self.DBS.add_dataframe_to_table(df=polars_dataframe,
                                            table_name=table_name,
                                            if_exists=operation)

        except Exception as Ex:
            logger.error(f"Возникла ошибка {Ex}")
            raise Ex


    def get_splits_history(self):
        """
        Получение информации о дроблении / консолидации бумаг фондового рынка

        :return
        """

        try:

            split_data_json = self.get_conn(url=self.split_url)

            if split_data_json == False:
                logger.error('Не удалось подключиться к API Мосбиржи для парсинга информации по сплитам')
                return False

            split_data_json_cutted = split_data_json['splits']['data']

            data = {}

            for i in range(len(split_data_json_cutted)):
                tradedate = self.str_to_datetime(split_data_json_cutted[i][0],
                                                 format_code="%Y-%m-%d")
                secid = split_data_json_cutted[i][1]
                quantity_before = split_data_json_cutted[i][2]
                quantity_after = split_data_json_cutted[i][3]

                data[secid] = [tradedate, quantity_before, quantity_after]

            df = pd.DataFrame([
                {
                    'date': values[0],
                    'secid': secid,
                    'quantity_before': values[1],
                    'quantity_after': values[2]
                }
                for secid, values in data.items()
            ])

            # Перевод в датафрейм поларс
            polars_dataframe = pl.from_pandas(df)

            # Сохранение в SQL
            self.DBS.add_dataframe_to_table(df=polars_dataframe,
                                            table_name='split_info',
                                            if_exists='replace')

            logger.info("Информация о дроблении / консолидации бумаг фондового рынка обновлена")
            return True

        except Exception as ex:
            logger.error(f'Возникла ошибка при получении информации о дроблении / консолидации \n {ex}')
            return False


    def get_changeover_history(self):
        """
        Получение информации по техническому изменению торговых кодов

        :return:
        """

        try:
            changeover_json = self.get_conn(url=self.rename_url)

            if changeover_json == False:
                logger.error('Не удалось подключиться к API Мосбиржи для парсинга информации по сплитам')
                return False

            # TODO: тут остановился

        except Exception as ex:
            logger.error(f'Возникла ошибка при получении информации о дроблении / консолидации \n {ex}')
            return False




t = Marketdata()
# t.get_current_info_shares_and_etfs()
# t.get_current_info_bonds()
# t.get_currencies()
# t.translate_to_rub()
# t.get_price_history(operation='replace', active_type='shares')
t.get_splits_history()

