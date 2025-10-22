from fastapi import FastAPI, Depends

from aol_stalker_2025.http.models import RootResponse, SearchResponse
from aol_stalker_2025.db.engine import AsyncSessionDependency, SessionDependency, get_session
from aol_stalker_2025.db.models.queries import Query
from sqlmodel import select, func

app = FastAPI()


@app.get("/")
def read_root() -> RootResponse:
    return RootResponse()

@app.get("/search")
def search(session: SessionDependency, literal: str | None = None, anon_id: int | None = None, query_id: int | None = None, limit: int = 100, offset: int = 0) -> SearchResponse:
    query = select(Query)
    if literal:
        query = query.where(func.to_tsvector('english', Query.query).match(literal))
    if anon_id:
        query = query.where(Query.anon_id == anon_id)
    if query_id:
        query = query.where(Query.id == query_id)
    query = query.where(Query.id >= offset).order_by(Query.id.asc()).limit(limit)
    results = session.exec(query).all()
    return SearchResponse(results=results)
