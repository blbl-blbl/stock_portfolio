import sqlite3
import logging
from typing import Optional, List, Dict, Any
import polars as pl

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager(object):
    def __init__(self, db_path: str = "database.db"):
        self.db_path = db_path

    def create_table(self, table_name: str, columns: Dict[str, str],
                     primary_key: str = None, foreign_keys: List[Dict] = None,
                     constraints: List[str] = None) -> bool:
        """
        Функция для создания таблицы

        Args:
            table_name (str): Название таблицы
            columns (Dict[str, str]): Словарь {название_столбца: тип_данных}
            primary_key (str, optional): Название столбца первичного ключа
            foreign_keys (List[Dict], optional): Список внешних ключей
            constraints (List[str], optional): Дополнительные ограничения

        Returns:
            bool: Успешно ли создана таблица
        """

        if self.table_exists(table_name=table_name):
            logger.error(f"Таблица с таким названием уже существует!")
            return False


        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Формируем SQL запрос
                sql_parts = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]

                # Добавляем столбцы
                column_definitions = []
                for col_name, col_type in columns.items():
                    column_def = f"{col_name} {col_type}"
                    if primary_key and col_name == primary_key:
                        column_def += " PRIMARY KEY AUTOINCREMENT"
                    column_definitions.append(column_def)

                sql_parts.append(", ".join(column_definitions))

                # Добавляем внешние ключи
                if foreign_keys:
                    for fk in foreign_keys:
                        fk_sql = f", FOREIGN KEY ({fk['column']}) REFERENCES {fk['references']}"
                        sql_parts.append(fk_sql)

                # Добавляем дополнительные ограничения
                if constraints:
                    for constraint in constraints:
                        sql_parts.append(f", {constraint}")

                sql_parts.append(")")
                sql = "".join(sql_parts)

                cursor.execute(sql)
                conn.commit()

                logger.info(f"Таблица '{table_name}' успешно создана")
                return True

        except sqlite3.Error as e:
            logger.error(f"Ошибка создания таблицы '{table_name}': {e}")
            return False

    def table_exists(self, table_name: str) -> bool:
        """Проверяет, существует ли таблица"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?
                """, (table_name,))
                return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Ошибка проверки существования таблицы: {e}")
            return False

    def get_table_columns(self, table_name: str) -> List[str]:
        """Возвращает список столбцов таблицы"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [column[1] for column in cursor.fetchall()]
                return columns
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения столбцов таблицы: {e}")
            return []

    def execute_safe(self, sql: str, params: tuple = ()) -> Optional[List]:
        """Безопасное выполнение SQL запроса"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)

                if sql.strip().upper().startswith('SELECT'):
                    return cursor.fetchall()
                else:
                    conn.commit()
                    return None

        except sqlite3.Error as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            return None

    def drop_table(self, table_name: str) -> bool:
        """
        Удаляет таблицу из базы данных

        Args:
            table_name (str): Название таблицы для удаления

        Returns:
            bool: Успешно ли удалена таблица
        """

        if not self.table_exists(table_name=table_name):
            logger.error(f"Попытка удаления несуществующей таблицы {table_name}!")
            return False

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                sql = f"DROP TABLE {table_name}"

                cursor.execute(sql)
                conn.commit()

                logger.info(f"Таблица '{table_name}' успешно удалена")
                return True

        except sqlite3.Error as e:
            logger.error(f"Ошибка удаления таблицы '{table_name}': {e}")
            return False

    def add_dataframe_to_table(self, df: pl.DataFrame, table_name: str,
                               if_exists: str = "append",
                               batch_size: int = 1000) -> bool:
        """
        Добавляет DataFrame Polars в таблицу SQL

        Args:
            df (pl.DataFrame): DataFrame Polars для добавления
            table_name (str): Название таблицы в базе данных
            if_exists (str): Действие при существующей таблице:
                            - "append": добавить данные (по умолчанию)
                            - "replace": удалить и пересоздать таблицу
            batch_size (int): Размер батча для вставки данных

        Returns:
            bool: Успешно ли выполнена операция
        """

        if df.is_empty():
            logger.warning("DataFrame пустой, нечего добавлять")
            return True

        # Проверяем существование таблицы
        table_exists = self.table_exists(table_name)

        try:
            with sqlite3.connect(self.db_path) as conn:
                # Если таблица существует и нужно заменить
                if table_exists and if_exists == "replace":
                    logger.info(f"Пересоздание таблицы '{table_name}'")
                    self.drop_table(table_name)
                    table_exists = False

                # Если таблицы нет - создаем
                if not table_exists:
                    # Создаем схему таблицы на основе типов данных Polars
                    columns = {}
                    for col in df.columns:
                        col_type = df[col].dtype

                        # Преобразование типов Polars в SQLite
                        if col_type in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32,
                                        pl.UInt64]:
                            sql_type = "INTEGER"
                        elif col_type in [pl.Float32, pl.Float64]:
                            sql_type = "REAL"
                        elif col_type == pl.Boolean:
                            sql_type = "INTEGER"  # SQLite не имеет типа BOOLEAN
                        elif col_type == pl.Date:
                            sql_type = "TEXT"  # Храним как текст в формате YYYY-MM-DD
                        elif col_type == pl.Datetime:
                            sql_type = "TEXT"  # Храним как текст
                        elif col_type == pl.Utf8:
                            sql_type = "TEXT"
                        else:
                            sql_type = "TEXT"  # По умолчанию TEXT

                        columns[col] = sql_type

                    # Создаем таблицу
                    if not self.create_table(table_name, columns):
                        logger.error(f"Не удалось создать таблицу '{table_name}'")
                        return False

                # Получаем список столбцов существующей таблицы
                table_columns = self.get_table_columns(table_name)

                # Проверяем соответствие столбцов
                df_columns = df.columns
                missing_columns = set(df_columns) - set(table_columns)
                extra_columns = set(table_columns) - set(df_columns)

                if missing_columns:
                    logger.warning(f"В таблице отсутствуют столбцы: {missing_columns}")
                    # Используем только столбцы, которые есть в таблице
                    df_columns_to_use = [col for col in df_columns if col in table_columns]
                    df = df.select(df_columns_to_use)
                elif extra_columns:
                    logger.warning(f"В таблице есть лишние столбцы: {extra_columns}")

                # Подготавливаем данные для вставки
                data_to_insert = []
                for row in df.iter_rows(named=True):
                    # Конвертируем специальные типы данных
                    processed_row = {}
                    for col, value in row.items():
                        if value is None:
                            processed_row[col] = None
                        elif isinstance(value, (pl.Date, pl.Datetime)):
                            processed_row[col] = str(value)
                        elif isinstance(value, bool):
                            processed_row[col] = int(value)
                        else:
                            processed_row[col] = value
                    data_to_insert.append(processed_row)

                # Вставляем данные батчами
                if data_to_insert:
                    columns_list = list(data_to_insert[0].keys())
                    placeholders = ", ".join(["?"] * len(columns_list))
                    columns_str = ", ".join(columns_list)

                    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

                    cursor = conn.cursor()

                    # Вставка батчами для больших DataFrame
                    for i in range(0, len(data_to_insert), batch_size):
                        batch = data_to_insert[i:i + batch_size]
                        batch_values = [tuple(row[col] for col in columns_list) for row in batch]

                        try:
                            cursor.executemany(insert_sql, batch_values)
                            conn.commit()
                            logger.info(
                                f"Успешно добавлено {len(batch)} записей в таблицу '{table_name}' (батч {i // batch_size + 1})")
                        except sqlite3.Error as e:
                            conn.rollback()
                            logger.error(f"Ошибка при вставке батча {i // batch_size + 1}: {e}")
                            return False

                logger.info(f"Успешно добавлено {len(data_to_insert)} записей в таблицу '{table_name}'")
                return True

        except Exception as e:
            logger.error(f"Ошибка при добавлении DataFrame в таблицу '{table_name}': {e}")
            return False

    def read_table_to_dataframe(self,
                                table_name: str = None,
                                sql_query: str = None,
                                columns: List[str] = None,
                                where_conditions: Dict[str, Any] = None,
                                limit: int = None) -> pl.DataFrame:
        """
        Выгружает данные из SQL таблицы в DataFrame Polars

        Args:
            table_name (str, optional): Название таблицы для выгрузки
            sql_query (str, optional): Произвольный SQL запрос для выполнения
            columns (List[str], optional): Список столбцов для выбора (если None - все столбцы)
            where_conditions (Dict[str, Any], optional): Условия WHERE в виде {столбец: значение}
            limit (int, optional): Ограничение количества строк

        Returns:
            pl.DataFrame: DataFrame с данными из базы данных

        Raises:
            ValueError: Если не указан table_name или sql_query
        """

        if table_name is None and sql_query is None:
            raise ValueError("Необходимо указать либо table_name, либо sql_query")

        try:
            with sqlite3.connect(self.db_path) as conn:
                if sql_query:
                    final_sql = sql_query
                    params = ()
                else:
                    if columns:
                        columns_str = ", ".join(columns)
                    else:
                        columns_str = "*"

                    final_sql = f"SELECT {columns_str} FROM {table_name}"
                    params = ()

                    if where_conditions:
                        where_clauses = []
                        where_values = []
                        for col, value in where_conditions.items():
                            # Если значение - кортеж, то первый элемент оператор, второй - значение
                            if isinstance(value, tuple) and len(value) == 2:
                                operator, actual_value = value
                                where_clauses.append(f"{col} {operator} ?")
                                where_values.append(actual_value)
                            else:
                                # По умолчанию используем =
                                where_clauses.append(f"{col} = ?")
                                where_values.append(value)

                        final_sql += " WHERE " + " AND ".join(where_clauses)
                        params = tuple(where_values)

                    if limit:
                        final_sql += f" LIMIT {limit}"

                df = pl.read_database(final_sql, conn, execute_options={"parameters": params})
                logger.info(f"Успешно загружено {len(df)} строк в DataFrame")
                return df

        except Exception as e:
            logger.error(f"Ошибка при выгрузке данных в DataFrame: {e}")
            return pl.DataFrame()

    def delete_row(self, table_name: str, where_conditions: Dict[str, Any]) -> bool:
        """
        Удаляет строки из таблицы по условиям

        Args:
            table_name (str): Название таблицы
            where_conditions (Dict[str, Any]): Условия WHERE в виде {столбец: значение}

        Returns:
            bool: Успешно ли выполнено удаление
        """

        if not self.table_exists(table_name):
            logger.error(f"Таблица '{table_name}' не существует!")
            return False

        if not where_conditions:
            logger.error("Не указаны условия для удаления!")
            return False

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Формируем условия WHERE
                where_clauses = []
                where_values = []

                for col, value in where_conditions.items():
                    # Поддержка различных операторов сравнения
                    if isinstance(value, tuple) and len(value) == 2:
                        operator, actual_value = value
                        where_clauses.append(f"{col} {operator} ?")
                        where_values.append(actual_value)
                    else:
                        # По умолчанию используем =
                        where_clauses.append(f"{col} = ?")
                        where_values.append(value)

                where_sql = " AND ".join(where_clauses)
                sql = f"DELETE FROM {table_name} WHERE {where_sql}"

                cursor.execute(sql, tuple(where_values))
                conn.commit()

                rows_affected = cursor.rowcount
                logger.info(f"Удалено {rows_affected} строк из таблицы '{table_name}'")
                return True

        except sqlite3.Error as e:
            logger.error(f"Ошибка удаления строк из таблицы '{table_name}': {e}")
            return False

    def update_row(self, table_name: str, update_data: Dict[str, Any],
                   where_conditions: Dict[str, Any]) -> bool:
        """
        Обновляет строки в таблице по условиям

        Args:
            table_name (str): Название таблицы
            update_data (Dict[str, Any]): Данные для обновления в виде {столбец: новое_значение}
            where_conditions (Dict[str, Any]): Условия WHERE в виде {столбец: значение}

        Returns:
            bool: Успешно ли выполнено обновление
        """

        if not self.table_exists(table_name):
            logger.error(f"Таблица '{table_name}' не существует!")
            return False

        if not update_data:
            logger.error("Не указаны данные для обновления!")
            return False

        if not where_conditions:
            logger.error("Не указаны условия для обновления!")
            return False

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Формируем часть SET для обновления
                set_clauses = []
                set_values = []

                for col, value in update_data.items():
                    set_clauses.append(f"{col} = ?")
                    set_values.append(value)

                set_sql = ", ".join(set_clauses)

                # Формируем условия WHERE
                where_clauses = []
                where_values = []

                for col, value in where_conditions.items():
                    # Поддержка различных операторов сравнения
                    if isinstance(value, tuple) and len(value) == 2:
                        operator, actual_value = value
                        where_clauses.append(f"{col} {operator} ?")
                        where_values.append(actual_value)
                    else:
                        # По умолчанию используем =
                        where_clauses.append(f"{col} = ?")
                        where_values.append(value)

                where_sql = " AND ".join(where_clauses)

                # Объединяем все значения для параметризованного запроса
                find_rowid_sql = f"SELECT rowid FROM {table_name} WHERE {where_sql} LIMIT 1"
                cursor.execute(find_rowid_sql, tuple(where_values))
                result = cursor.fetchone()

                if not result:
                    logger.warning(f"Не найдено записей для обновления в таблице '{table_name}'")
                    return False

                rowid = result[0]

                # Обновляем только запись с найденным ROWID
                sql = f"UPDATE {table_name} SET {set_sql} WHERE rowid = ?"
                all_values = set_values + [rowid]

                cursor.execute(sql, tuple(all_values))
                conn.commit()

                rows_affected = cursor.rowcount
                logger.info(f"Обновлено {rows_affected} строк в таблице '{table_name}'")
                return True

        except sqlite3.Error as e:
            logger.error(f"Ошибка обновления строк в таблице '{table_name}': {e}")
            return False


if __name__ == '__main__':
    test = DatabaseManager()
    # test.create_table(table_name='tes',
    #                   columns={'id': 'INTEGER',
    #                            'mimimi': 'TEXT'},
    #                   primary_key='id',
    #                   )
    # test.drop_table(table_name='test_table')
    # data = {"col1": [0, 2], "col2": [3, 7]}
    # df2 = pl.DataFrame(data, schema={"col1": pl.Float32, "col2": pl.Int64})
    # test.add_dataframe_to_table(df=df2, table_name='test')



    # выгрузка из sql
    print(test.read_table_to_dataframe(table_name='operations_history'))

    # Удаление по значению строки
    from datetime import date
    test.delete_row(table_name='operations_history',
                    where_conditions={
                        "Date" : date(year=2025, month=11, day=3),
                        "SECID" : 'KILL',
                        "Operation" : 'buy',
                        "Quantity" : 10,
                        "Price" : 100.0
                    })
    print(test.read_table_to_dataframe(table_name='operations_history'))

