import sqlite3
import logging
from typing import Optional, List, Dict

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, db_path: str = "database.db"):
        self.db_path = db_path

    def create_table(self, table_name: str, columns: Dict[str, str],
                     primary_key: str = None, foreign_keys: List[Dict] = None,
                     constraints: List[str] = None) -> bool:
        """
        Универсальная функция для создания таблицы

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


if __name__ == '__main__':
    test = DatabaseManager()
    # test.create_table(table_name='tes',
    #                   columns={'id': 'INTEGER',
    #                            'mimimi': 'TEXT'},
    #                   primary_key='id',
    #                   )
    test.drop_table(table_name='test_table')

