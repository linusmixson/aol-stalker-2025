from pydantic import ConfigDict, Field
from pydantic_settings import BaseSettings
from sqlalchemy.engine.url import make_url, URL


class Config(BaseSettings):
    postgres_connection_string: str = Field(
        description="PostgreSQL connection string",
        default="postgresql://postgres:@localhost:5432/aol_stalker_2025",
    )

    model_config = ConfigDict(
        env_file=".env",
        extra="ignore"
    )

    def asyncpg_postgres_connection_url(self, ssl: bool = False) -> URL:
        url = make_url(self.postgres_connection_string)
        url = url.set(drivername="postgresql+asyncpg")
        if ssl:
            url = url.update_query_dict({"ssl": "true"})
        return url

    def psycopg_postgres_connection_url(self, ssl: bool = False) -> URL:
        url = make_url(self.postgres_connection_string)
        url = url.set(drivername="postgresql+psycopg")
        if ssl:
            url = url.update_query_dict({"sslmode": "required"})
        return url


config = Config()
