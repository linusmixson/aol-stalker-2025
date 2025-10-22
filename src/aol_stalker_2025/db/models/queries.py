from datetime import datetime

from sqlmodel import Field, SQLModel
from pgvector.sqlalchemy import Vector
from pydantic import ConfigDict


class Query(SQLModel, table=True):
    __tablename__ = "queries"
    id: int = Field(primary_key=True)
    anon_id: int
    query: str
    query_time: datetime
    item_rank: int | None
    click_url: str | None
