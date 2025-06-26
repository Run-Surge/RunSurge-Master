from pydantic_settings import BaseSettings, SettingsConfigDict
from urllib.parse import quote_plus
from typing import Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    SECRET_KEY: str
    ALGORITHM: Optional[str] = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: Optional[int] = 3600000
    REFRESH_TOKEN_EXPIRE_DAYS: Optional[int] = 7

    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_NAME: str
    DB_PORT: int

    MASTER_PORT: Optional[int] = 8000
    GRPC_IP: Optional[str] = "10.10.10.219"
    GRPC_PORT: Optional[int] = 12345

    @property
    def DATABASE_URL(self) -> str:
        # It's crucial to URL-encode the password
        encoded_password = quote_plus(self.DB_PASSWORD)
        return f"postgresql+asyncpg://{self.DB_USER}:{encoded_password}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

settings = Settings()   