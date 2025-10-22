import psycopg
import voyageai
from pydantic import ConfigDict, Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    postgres_url: str = Field(
        description="PostgreSQL connection string",
        default="postgresql://postgres:@localhost:5432/aol_stalker_2025",
    )
    query: str = Field(
        description="Query to execute",
        default="athletics",
    )
    voyageai_api_key: str = Field(
        description="VoyageAI API key",
    )
    voyageai_model: str = Field(
        description="VoyageAI model to use",
        default="voyage-3.5-lite",
    )
    max_results: int = Field(
        description="Maximum number of results to return",
        default=10,
    )

    model_config = ConfigDict(
        env_file=".env",
        cli_parse_args=True,
        cli_kebab_case=True,
    )


def main() -> None:
    settings = Settings()
    voyageai_client = voyageai.Client(api_key=settings.voyageai_api_key)
    embedding_result = voyageai_client.embed(
        [settings.query], model=settings.voyageai_model, input_type="query"
    )
    embedding = embedding_result.embeddings[0]
    inner = ",".join(str(e) for e in embedding)
    with (
        psycopg.connect(settings.postgres_url) as connection,
        connection.cursor() as cursor,
    ):
        inner = ",".join(str(e) for e in embedding)
        cursor.execute(
            "SELECT q.id, q.query FROM queries AS q "
            "JOIN embeddings_voyageai_3_5_lite AS e "
            "ON q.id = e.queries_id WHERE q.query != '-' "
            "ORDER BY e.embedding <-> %s",
            (f"[{inner}]",),
        )
        results = cursor.fetchall()
        for result in results[: settings.max_results]:
            print(result)  # noqa: T201


if __name__ == "__main__":
    main()
