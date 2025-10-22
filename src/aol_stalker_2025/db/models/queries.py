from datetime import datetime

from sqlmodel import Field, SQLModel


class Query(SQLModel, table=True):
    __tablename__ = "queries"
    id: int = Field(primary_key=True)
    anon_id: int
    query: str
    query_time: datetime
    item_rank: int | None
    click_url: str | None
