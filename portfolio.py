import polars as pl
import logging


# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Portfolio(object):
    def __init__(self):
        pass

    def excel_to_df(self, path: str):
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

        # Переименовывание столбцов в нужные
        new_columns = ['Date', 'SECID', 'Operation', 'Quantity', 'Price']
        old_columns = w_df.columns

        for i in range(len(new_columns)):
            w_df = w_df.rename({old_columns[i] : new_columns[i]})

        # Проверка файла на соотвествие типам данных
        try:
            w_df = w_df.with_columns(
                pl.col('Date').str.to_date(format='%m-%d-%y')
            )
        except Exception as e:
            logger.error(f"Ошбика при конвертации {new_columns[0]}. \n{e}")
            raise ValueError (f"Столбец {old_columns[0]} должен быть датой")

        try:
            w_df.cast({"Quantity" : pl.Int64})
        except Exception as e:
            logger.error(f"Ошбика при конвертации {new_columns[3]}. \n{e}")
            raise ValueError(f"Столбец {old_columns[3]} должен быть целочисленным значением")

        try:
            w_df.cast({"Price" : pl.Float64})
        except Exception as e:
            logger.error(f"Ошбика при конвертации {new_columns[4]}. \n{e}")
            raise ValueError(f"Столбец {old_columns[4]} должен быть представлен числовыми значениеми")


        # Возможные значения для столбца 'Operation'
        available_operations = ['buy', 'sell', 'купить', 'продать', 'купила',
                                'продала', 'шорт', 'лонг', 'short', 'long',
                                'купил', 'продал']

        # Проверка, что в стоблце 'Operation' нет неопознанных значений
        invalid_rows = w_df.filter(
            ~pl.col('Operation')
            .str.strip_chars() # удаляет пробелы справа и слева
            .str.to_lowercase() # приводит к нижнему регистру
            .is_in(available_operations)
        )

        # Если существуют строки с неопознанными операциями, то показываем в каких строках ошибки и
        # возвращаем None
        if not invalid_rows.is_empty():
            logger.warning("Найдены строки с неопозанными значениями в столбце 'Operation'")
            print(f"Найдены строки с неопозанными значениями в столбце '{old_columns[2]}'")
            print(invalid_rows)
            return None

        return w_df

    def operations_history_to_sql(self, operation : str, path: str = None, data : pl.DataFrame = None):
        """
        Запись данных из DataFrame в SQL

        :param data: DataFrame Polars для добавления / замены в SQL
        :param operation: Тип действия
            - 'append' : добавить к тому что существует, если не существует, будет создано
            - 'replace' : заменить существующую таблицу на новые данные
        :param path: Путь до Excel файла
            - None : добавление данных не из Excel
            - Not None : добавлениие данных из Excel (нужен путь до файла)
        :return:
        """

        if path is not None:
            df = self.excel_to_df(path=path)
        else:
            # Логика обработки когда в историю добавляется не excel
            pass

        # Проверка файла на соответствие нужной структуре
        df = self.excel_check(df=df)


        if operation == 'replace':
            # Логика обработки при замене существующей таблицы
            pass
        elif operation == 'append':
            # Логика обработки при добавлении в таблицу
            pass





if __name__ == "__main__":
    port = Portfolio()
    df = port.excel_to_df(path='port.xlsx')
    port.excel_check(df=df)
