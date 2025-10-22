import asyncio
import csv
import datetime
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

import asyncpg
from pydantic import ConfigDict, Field
from pydantic_settings import BaseSettings

from aol_stalker_2025.util import static_root

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = getLogger(__name__)


class DataRow(NamedTuple):
    anon_id: str
    query: str
    query_time: str
    item_rank: int
    click_url: str

    @classmethod
    def convert(cls, row: tuple[str, str, str, str, str]) -> DataRow:
        return DataRow(
            anon_id=int(row[0]),
            query=row[1],
            query_time=datetime.datetime.fromisoformat(row[2]),
            item_rank=int(row[3]) if row[3] else None,
            click_url=row[4] if row[4] else None,
        )


class Settings(BaseSettings):
    postgres_url: str = Field(
        description="Fully-qualified PostgreSQL connection string",
        default="postgresql://postgres:@localhost:5432/aol_stalker_2025",
    )
    data_dir: Path = Field(
        description="Directory containing the data files",
        default_factory=lambda: static_root() / "data",
    )
    glob: str = Field(
        description="Glob pattern for the data files",
        default="user-ct-test-collection-*.txt",
    )
    delimiter: str = Field(
        description="Delimiter for the data files",
        default="\t",
    )
    table_name: str = Field(
        description="Name of the table to import the data into",
        default="queries",
    )

    model_config = ConfigDict(
        env_file=".env",
        cli_parse_args=True,
        cli_kebab_case=True,
    )


class Importer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def get_files(self, root: Path, glob: str | None = None) -> Iterable[Path]:
        return root.glob(glob or self.settings.glob)

    def get_reader(
        self, file_path: Path, delimiter: str | None = None
    ) -> Iterable[DataRow]:
        with file_path.open("r") as f:
            reader = csv.reader(f, delimiter=delimiter or self.settings.delimiter)
            next(reader)  # skip header
            yield from (DataRow.convert(tuple(row)) for row in reader)

    def get_readers(
        self, root: Path, glob: str | None = None
    ) -> Iterable[Iterable[DataRow]]:
        for file_path in self.get_files(root, glob):
            yield self.get_reader(file_path)

    async def copy_batch(
        self,
        data_rows: Iterable[DataRow],
        connection: asyncpg.Connection,
        table_name: str | None = None,
    ) -> None:
        await connection.copy_records_to_table(
            table_name or self.settings.table_name,
            records=data_rows,
            columns=[
                "anon_id",
                "query",
                "query_time",
                "item_rank",
                "click_url",
            ],
        )

    async def import_data(self, root: Path, connection: asyncpg.Connection) -> None:
        logger.info("Importing data from %s", root)
        for i, reader in enumerate(self.get_readers(root)):
            logger.info("Copying batch %s", i)
            await self.copy_batch(reader, connection)
            logger.info("Copied batch %s", i)

    async def main(self) -> None:
        connection = await asyncpg.connect(self.settings.postgres_url)
        await self.import_data(self.settings.data_dir, connection)
        await connection.close()


if __name__ == "__main__":
    importer = Importer(Settings())
    asyncio.run(importer.main())
