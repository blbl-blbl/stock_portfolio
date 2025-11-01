import docker
import psycopg2
import time
import logging
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Нужно добавить чтобы переменные container_name, password и port считывались из .env файла
class DockerPostgresManager:
    def __init__(self,
                 container_name="postgres_python",
                 password="mysecretpassword",
                 port=5432,
                 database="postgres"):
        self.client = docker.from_env()
        self.container_name = container_name
        self.password = password
        self.port = port
        self.database = database
        self.container = None

    def is_container_running(self):
        """Проверяет, запущен ли контейнер"""
        try:
            self.container = self.client.containers.get(self.container_name)
            return self.container.status == "running"
        except docker.errors.NotFound:
            return False

    def start_postgres_container(self):
        """Запускает PostgreSQL в Docker контейнере"""
        try:
            if self.is_container_running():
                logger.info("PostgreSQL контейнер уже запущен")
                return True

            # Останавливаем и удаляем старый контейнер если существует
            try:
                old_container = self.client.containers.get(self.container_name)
                old_container.remove(force=True)
            except docker.errors.NotFound:
                pass

            # Запускаем новый контейнер
            self.container = self.client.containers.run(
                "postgres:13",
                name=self.container_name,
                environment={
                    "POSTGRES_PASSWORD": self.password,
                    "POSTGRES_DB": self.database,
                    "POSTGRES_USER": "postgres"
                },
                ports={'5432/tcp': self.port},
                detach=True,
                remove=True  # Автоматически удаляем при остановке
            )

            # Ждем запуска PostgreSQL
            logger.info("Ожидаем запуск PostgreSQL...")
            time.sleep(10)

            # Проверяем статус
            for _ in range(10):
                if self.check_connection():
                    logger.info("PostgreSQL успешно запущен в Docker")
                    return True
                time.sleep(2)

            logger.error("Не удалось подключиться к PostgreSQL")
            return False

        except Exception as e:
            logger.error(f"Ошибка при запуске контейнера: {e}")
            return False

    def check_connection(self):
        """Проверяет подключение к PostgreSQL"""
        try:
            conn = psycopg2.connect(
                host="localhost",
                user="postgres",
                password=self.password,
                port=self.port,
                database=self.database,
                connect_timeout=5
            )
            conn.close()
            return True
        except Exception:
            return False

    def stop_container(self):
        """Останавливает контейнер"""
        try:
            if self.container:
                self.container.stop()
                logger.info("Контейнер остановлен")
        except Exception as e:
            logger.error(f"Ошибка при остановке контейнера: {e}")

    def create_database(self, db_name):
        """Безопасно создает базу данных"""
        if not self.start_postgres_container():
            logger.error("Не удалось запустить PostgreSQL")
            return False

        try:
            conn = psycopg2.connect(
                host="localhost",
                user="postgres",
                password=self.password,
                port=self.port,
                database=self.database
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as cursor:
                # Проверяем существование БД
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (db_name,)
                )
                exists = cursor.fetchone()

                if not exists:
                    # Создаем БД
                    create_query = sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(db_name)
                    )
                    cursor.execute(create_query)
                    logger.info(f"База данных '{db_name}' создана успешно")
                    return True
                else:
                    logger.info(f"База данных '{db_name}' уже существует")
                    return True

        except Exception as e:
            logger.error(f"Ошибка при создании БД: {e}")
            return False
        finally:
            if 'conn' in locals():
                conn.close()

    def execute_query(self, query, params=None, db_name=None):
        """Выполняет SQL запрос"""
        if not self.check_connection():
            logger.error("Нет подключения к PostgreSQL")
            return None

        try:
            conn = psycopg2.connect(
                host="localhost",
                user="postgres",
                password=self.password,
                port=self.port,
                database=db_name or self.database
            )

            with conn.cursor() as cursor:
                cursor.execute(query, params)

                if query.strip().lower().startswith('select'):
                    result = cursor.fetchall()
                    conn.close()
                    return result
                else:
                    conn.commit()
                    conn.close()
                    return cursor.rowcount

        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            return None