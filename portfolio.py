import polars as pl
import logging
from database import DatabaseManager
from datetime import date
from typing import List


# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Portfolio(object):
    def __init__(self):
        self.DatabaseManager = DatabaseManager()
        # Возможные значения для столбца 'Operation'
        self.available_sell_operations = ['sell', 'продать','продала', 'шорт', 'short', 'продал']
        self.available_buy_operations = ['buy', 'купить', 'купила', 'лонг', 'long','купил']

    @staticmethod
    def excel_to_df(path: str):
        """ Чтение файла из Excel """
        try:
            df = pl.read_excel(
                source = path,
                sheet_id = 1, # Используем первый лист из файла
                engine = "xlsx2csv")
            logger.info(f"Файл по пути {path} был загружен")
            return df

        except Exception as e:
            logger.error(f"Файл не пути {path} не найден")
            raise e

    def excel_check(self, df: pl.DataFrame):
        """
        Проверка файла Excel на соответствие нужной структуре
        Нужная структура:
            - 1 столбец: дата операции
            - 2 столбец: тикер / ISIN
            - 3 столбец: операция с бумагой (buy / sell / купить / продать)
            - 4 столбец: количество бумаг (в штуках, НЕ в лотах)
            - 5 столбец: цена по которой была операция

        :return: DataFrame Polars с унифицированными столбцами и правильными типами данных
        """

        # Проверка количества столбцов
        if len(df.get_columns()) != 5:
            logger.error("В передаваемом Excel-файле количество столбцов не соответствует 5")
            raise ValueError ("Количество столбцов не соответствует нужному!")

        w_df = df.clone()

        # Удаление пустых строк
        w_df = w_df.drop_nulls()

        if df.height != w_df.height:
            logger.warning("Были удалены пустые строки")
        else:
            logger.info("Пустые строки не обнаружены")


        # Переименовывание столбцов в нужные
        new_columns = ['Date', 'SECID', 'Operation', 'Quantity', 'Price']
        old_columns = w_df.columns

        for i in range(len(new_columns)):
            w_df = w_df.rename({old_columns[i] : new_columns[i]})


        # Проверка файла на соотвествие типам данных
        w_df = self.typization(df = w_df, types=['Date', 'String', 'String', 'Int64', 'Float64'])



        # Проверка, что в стоблце 'Operation' нет неопознанных значений
        self.operation_check(w_df)


        # Изменение значений количества на отрицательные где есть sell
        # Если sell, то в Quantity ставится минус, если buy, то плюс
        w_df = w_df.with_columns(
            pl.when(pl.col('Operation').is_in(self.available_sell_operations))
            .then(pl.col('Quantity') * -1)  # делаем отрицательным
            .otherwise(pl.col('Quantity'))  # оставляем как есть
            .alias('Quantity')
        )

        return w_df

    def operation_check(self, df : pl.DataFrame):
        """
        Проверка доступности типа операции
        :param df: DataFrame со столбцом 'Operation'
        :return: True или ValueError
        """
        invalid_rows = df.filter(
            ~pl.col('Operation')
            .str.strip_chars() # удаляет пробелы справа и слева
            .str.to_lowercase() # приводит к нижнему регистру
            .is_in(self.available_sell_operations + self.available_buy_operations)
        )

        # Если существуют строки с неопознанными операциями, то показываем в каких строках ошибки и
        # возвращаем False
        if not invalid_rows.is_empty():
            logger.error("Найдены строки с неопозанными значениями в столбце 'Operation'")
            print(invalid_rows)
            raise ValueError (f"Найдены строки с неопозанными значениями в столбце 'Operation'")

        return True

    @staticmethod
    def typization(df: pl.DataFrame, types: List[str]):
        """
        Изменяет типы данных в DataFrame
        :param df: DateFrame в котром нужно изменить типы
        :param types: Список типов на которые необходимо изменить
        :return: DateFrame с измененными типами данных
        """

        # Возможные типы в Polars
        valid_types = ["Int64", "Int32", "Float64", "Float32", "String", "Boolean", "Date", "Datetime"]

        # Проверка, что все запрашиваемые типы существуют
        for i in types:
            if i not in valid_types:
                logger.error(f"Попытка преобразованиия неизвестного типа данных {i}")
                raise ValueError (f"Неизвестный тип данных {i}")

        # Названия столбцов в DateFrame
        df_columns = df.columns

        # Конвертация типов
        for t in range(len(types)):
            try:
                if types[t] == 'Date':
                    df = df.with_columns(pl.col('Date').str.to_date(format='%m-%d-%y')) # format='%Y-%m-%d'
                else:
                    df = df.cast({df_columns[t] : getattr(pl, types[t])})
            except Exception as e:
                logger.error(f"Ошибка при конвертации столбца {df_columns[t]} в формат {types[t]}")
                raise ValueError(f"Ошибка при конвертации столбца {df_columns[t]} в формат {types[t]} \n {e}")

        return df

    def operations_history_to_sql(self, operation : str, path: str = None, df : pl.DataFrame = None):
        """
        Запись данных из DataFrame в SQL

        :param df: DataFrame Polars для добавления / замены в SQL
        :param operation: Тип действия
            - 'append' : добавить к тому что существует, если не существует, будет создано
            - 'replace' : заменить существующую таблицу на новые данные
        :param path: Путь до Excel файла
            - None : добавление данных не из Excel
            - Not None : добавлениие данных из Excel (нужен путь до файла)
        :return:
        """

        if path is None and df is None or path is not None and df is not None:
            logger.error("В функцию operations_history_to_sql переданы оба параметра path и df, а должен быть только один")
            raise ValueError ("Должен быть передан только один из параматеров: path или df")

        if path is not None:
            df = self.excel_to_df(path=path)


        # Проверка файла на соответствие нужной структуре
        df = self.excel_check(df=df)


        if operation == 'replace':
            # Логика обработки при замене существующей таблицы
            self.DatabaseManager.add_dataframe_to_table(df=df,
                                                        table_name='operations_history',
                                                        if_exists='replace')
        elif operation == 'append':
            # Логика обработки при добавлении в таблицу
            self.DatabaseManager.add_dataframe_to_table(df=df,
                                                        table_name='operations_history',
                                                        if_exists='append')

    @staticmethod
    def quantity_for_active(data: pl.DataFrame, target_date: date = date.today()):
        """
        Определяем количество бумаг в портфеле на текущий момент

        :param target_date: Дата на которую считается количество бумаг
        :param data: DataFrame с историей операций
        :return: DataFrame с количеством каждого актива на дату
        """

        # Преобразование даты в "понятный" для Polars тип
        target_date = target_date.strftime('%Y-%m-%d')

        # Определение количества каждого актива на дату
        t_data = data.filter(pl.col("Date") <= target_date).group_by("SECID").agg(pl.col('Quantity').sum())

        # Удаление активов где Quantity = 0
        t_data = t_data.filter(pl.col('Quantity') != 0)

        return t_data

    def add_new_operation(self, secid :str,
                          operation_type : str,
                          quantity : int,
                          price : float,
                          operation_date: date = date.today()):
        """
        Добавление единичной операции в историю операций

        :param secid: униальный id актива
        :param operation_type: тип оперции (buy / sell)
        :param quantity: количество активов
        :param price: цена единицы актива
        :param operation_date: дата операции (по умолчанию - сегодня)
        :return:
        """

        operation_type = operation_type.lower().strip()

        # Проверка типа операции
        if operation_type not in (self.available_sell_operations + self.available_buy_operations):
            logger.error(f"Неопознанный тип операции {operation_type}")
            raise ValueError (f"Неопознанный тип операции {operation_type}")

        add_row = pl.DataFrame({
           'Date' : operation_date,
            'SECID' : secid,
            'Operation' : operation_type,
            'Quantity' : quantity,
            'Price' : price
        })

        # Добавление в SQL
        self.DatabaseManager.add_dataframe_to_table(df=add_row,
                                                    table_name='operations_history',
                                                    if_exists='append')

    def operations_history_by_period(self, start_date: date, end_date: date = None) -> pl.DataFrame:
        """
        Выгружает историю операций за выбранный период
        :param start_date: Начальная дата (формат date)
        :param end_date: Конечная дата (по умолчанию = начальная) (формат date)
        :return: DataFrame Polars с историей операций
        """


        if end_date is None:
            end_date = start_date
        # Проверка на адекватность дат
        elif  start_date > end_date:
            logger.error(f"Передана end_date меньше чем start_date: end_date: {end_date} vs start_date {start_date}")
            raise ValueError (f"Передана end_date меньше чем start_date: end_date: {end_date} vs start_date {start_date}")

        # Выгрузка
        df = self.DatabaseManager.read_table_to_dataframe(
            sql_query=f"SELECT * FROM operations_history WHERE Date >= '{start_date}' AND Date <= '{end_date}'"
        )

        df = df.with_row_index(name='№', offset=1)

        logger.info(f"Получены данные по операциям за период {start_date} - {end_date}")

        return df

    @staticmethod
    def get_row_by_index(df: pl.DataFrame, index: int) -> dict:
        """
        Получение строки по индексу, работает в паре с operations_history_by_period
        :param df: DataFrame Polars
        :param index: Номер строки (нумерация с 1)
        :return: строка в формате dict
        """

        # Проверка на существование строки
        if df.height < index or index < 1:
            logger.error(f"Введен неверный индекс {index}. Строк в DataFrame {df.height}")
            raise ValueError ("Введен неверный индекс")

        row_dict = df.row(index=index-1, named=True) # нужная строка в формате Dict

        # row_df = pl.DataFrame(row_dict) # нужная строка в формате DataFrame Polars

        logger.info(f"Выведена {index} строка из {df.height}")

        return row_dict

    def delete_row(self, row: dict):
        """

        :param row: Строка в формате dict которая будет удалена
        :return: None
        """

        try:
            self.DatabaseManager.delete_row(
                table_name="operations_history",
                where_conditions= {
                    "Date" : row["Date"],
                    "SECID" : row["SECID"],
                    "Operation" : row["Operation"],
                    "Quantity" : row["Quantity"],
                    "Price" : row["Price"]
                }
            )
        except Exception as e:
            logger.error(f"Возникла ошибка при удалении строки {row}")
            raise e



if __name__ == "__main__":
    port = Portfolio()
    # port.operations_history_to_sql(operation='replace', path='port.xlsx')
    # data = port.DatabaseManager.read_table_to_dataframe(table_name='operations_history')
    # print(data)
    # port.quantity_for_active(data=data, target_date=date(year=2025, month=8, day=21))
    # print(port.quantity_for_active(data=data))
    # port.add_new_operation(secid='KILL', operation_type='buy', quantity=10, price=100, operation_date=date(year=2025, month=11, day=2)) # Добавлнеие единичной записи
    t = port.operations_history_by_period(start_date=date(year=2025, month=8, day=2), end_date=date.today())
    print(t)
