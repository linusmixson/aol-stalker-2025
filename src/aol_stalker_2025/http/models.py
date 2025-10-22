from typing import Literal

from pydantic import BaseModel, Field

from aol_stalker_2025.db.models.queries import Query


class RootResponse(BaseModel):
    message: Literal["Hello, world!"] = "Hello, world!"


class SearchResponse(BaseModel):
    results: list[Query] = Field(description="The results of the search")
