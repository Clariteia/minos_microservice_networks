"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    NoReturn,
)

from minos.common import (
    PostgreSqlMinosDatabase,
)


class SnapshotSetup(PostgreSqlMinosDatabase):
    """Minos Snapshot Setup Class"""

    async def _setup(self) -> NoReturn:
        await self.submit_query(_CREATE_TABLE_QUERY)
        await self.submit_query(_CREATE_OFFSET_TABLE_QUERY)

    async def _load_offset(self) -> int:
        # noinspection PyBroadException
        try:
            raw = await self.submit_query_and_fetchone(_SELECT_OFFSET_QUERY)
            return raw[0]
        except Exception:
            return 0


_CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS snapshot (
    aggregate_id BIGINT NOT NULL,
    aggregate_name TEXT NOT NULL,
    version INT NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (aggregate_id, aggregate_name)
);
""".strip()

_CREATE_OFFSET_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS snapshot_aux_offset (
    id bool PRIMARY KEY DEFAULT TRUE,
    value BIGINT NOT NULL,
    CONSTRAINT id_uni CHECK (id)
);

""".strip()
_SELECT_OFFSET_QUERY = """
SELECT value
FROM snapshot_aux_offset
WHERE id = TRUE;
"""
