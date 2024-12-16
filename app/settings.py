# app/settings.py
from pydantic_settings import BaseSettings
from pydantic import PostgresDsn


class AsyncSettings(BaseSettings):
    """
    Класс настроек для асинхронного подключения к базе данных и Redis.

    Атрибуты:
        db_name (str): Название базы данных.
        db_host (str): Хост базы данных.
        db_port (str): Порт базы данных.
        db_user (str): Пользователь базы данных.
        db_pass (str): Пароль базы данных.
        redis_url (str): URL подключения к Redis.
    """
    db_name: str
    db_host: str
    db_port: str
    db_user: str
    db_pass: str
    redis_url: str

    class Config:
        env_file = ".env"
        case_sensitive = True

    @property
    def async_database_url(self) -> str:
        """
        Формирует URL для подключения к базе данных PostgreSQL.

        Returns:
            str: URL подключения к базе данных.
        """
        return f"postgresql+asyncpg://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"


async_settings = AsyncSettings()
