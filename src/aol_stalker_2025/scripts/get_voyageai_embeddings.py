import asyncio
from collections.abc import Iterable

import psycopg
import voyageai
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    postgres_url: str = Field(
        description="PostgreSQL connection string",
        default="postgresql://postgres:@localhost:5432/aol_stalker_2025",
    )
    source_table: str = Field(
        description="Name of the table to get the data from",
        default="queries",
    )
    target_table: str = Field(
        description="Name of the table to store the embeddings in",
        default="embeddings_voyageai_3_5_lite",
    )
    voyageai_api_key: str = Field(
        description="VoyageAI API key",
        default="pa-jpf6g6lGu7_pbQjJJ79tdqIeeLckqQpQpU6xOxJgrB2",
    )
    voyageai_model: str = Field(
        description="VoyageAI model to use",
        default="voyage-3.5-lite",
    )
    max_rows: int | None = Field(
        description="Maximum number of rows to embed",
        default=None,
    )
    batch_size: int = Field(
        description="Batch size to use for embedding",
        default=1,
    )

    model_config = ConfigDict(
        env_file=".env",
        cli_parse_args=True,
        cli_kebab_case=True,
    )

    def get_voyageai_client(self) -> voyageai.AsyncClient:
        return voyageai.Client(api_key=self.voyageai_api_key)


class QueryRow(BaseModel):
    queries_id: int
    query: str


class EmbeddedQuery(BaseModel):
    queries_id: int
    embedding: list[float]


class Embedder:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.voyageai_client = self.settings.get_voyageai_client()
        self.cache = {}

    def get_rows(self, connection: psycopg.Connection) -> Iterable:
        query = "SELECT id, query FROM %s WHERE id > 130700 ORDER BY id ASC"
        if self.settings.max_rows:
            query += f" LIMIT {self.settings.max_rows}"
        connection.execute(
            "SET SESSION CHARACTERISTICS AS TRANSACTION "
            "ISOLATION LEVEL REPEATABLE READ READ ONLY"
        )
        connection.execute(
            "SET statement_timeout = 0"
        )  # opt: ensure no timeout while fetching
        with connection.cursor(name="stream_all") as cursor:
            yield from cursor.stream(query, (self.settings.source_table,), size=100)

    def get_batches(
        self, rows: Iterable[tuple[int, str]]
    ) -> Iterable[list[tuple[int, str]]]:
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) == self.settings.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def embed(self, queries_id: int, query: str) -> EmbeddedQuery:
        if query in self.cache:
            return EmbeddedQuery(queries_id=queries_id, embedding=self.cache[query])
        # You can pass more than one item of text to the embed method at a time…
        # but for some reason, this results in different embeddings than
        # you get doing them one at a time. I haven't figured out why this is yet.
        embedding_result = self.voyageai_client.embed(
            [query], model=self.settings.voyageai_model, input_type=None
        )
        embeddings = embedding_result.embeddings
        embedding = EmbeddedQuery(queries_id=queries_id, embedding=embeddings[0])
        self.cache[query] = embedding.embedding
        return embedding

    def save_embeddings(self, embeddings: Iterable[EmbeddedQuery]) -> None:
        with (
            psycopg.connect(self.settings.postgres_url) as connection,
            connection.cursor() as cursor,
        ):
            cursor.executemany(
                "INSERT INTO embeddings_voyageai_3_5_lite (queries_id, embedding) "
                "VALUES (%s, %s)",
                [
                    (embedding.queries_id, embedding.embedding)
                    for embedding in embeddings
                ],
            )

    async def main(self) -> None:
        with psycopg.connect(self.settings.postgres_url) as connection:
            rows = self.get_rows(connection)
            print("Got row scroller")  # noqa: T201
            batches = self.get_batches(rows)
            print("Got batches")  # noqa: T201
            # This is obviously massively inefficient. Unfortunately,
            # VoyageAI has pretty tight rate limits, and this serial
            # approach stays south of them.
            for i, batch in enumerate(batches):
                print(f"Processing batch {i}")  # noqa: T201
                print(f"Getting embeddings for batch {i}")  # noqa: T201
                embeddings = [self.embed(row[0], row[1]) for row in batch]
                print(f"Got embeddings for batch {i}")  # noqa: T201
                print(f"Saving embeddings for batch {i}")  # noqa: T201
                self.save_embeddings(embeddings)
                print("Saved embeddings for batch {i}")  # noqa: T201
                print(f"Processsed batch {i}")  # noqa: T201


if __name__ == "__main__":
    embedder = Embedder(Settings())
    asyncio.run(embedder.main())
