"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    AsyncIterator,
)

from minos.common import (
    Aggregate,
    MinosConfig,
)

from .abc import (
    SnapshotSetup,
)
from .builders import (
    SnapshotBuilder,
)
from .entries import (
    SnapshotEntry,
)


class SnapshotReader(SnapshotSetup):
    """Minos Snapshot Reader class."""

    builder: SnapshotBuilder

    def __init__(self, *args, builder: SnapshotBuilder, **kwargs):
        super().__init__(*args, **kwargs)
        self.builder = builder

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> SnapshotReader:
        builder = SnapshotBuilder.from_config(*args, config=config, **kwargs)
        return cls(*args, builder=builder, **config.snapshot._asdict(), **kwargs)

    async def get(self, aggregate_name: str, ids: list[int], **kwargs) -> AsyncIterator[Aggregate]:
        """TODO

        :param aggregate_name: TODO
        :param ids: TODO
        :param kwargs: TODO
        :return: TODO
        """
        # noinspection PyShadowingBuiltins
        if not await self.builder.are_synced(aggregate_name, ids):
            await self.builder.dispatch()

        async for item in self._get(aggregate_name, ids):
            yield item.aggregate

    async def _get(self, aggregate_name: str, ids: list[int]) -> AsyncIterator[SnapshotEntry]:
        count = 0
        async for row in self.submit_query_and_iter(_SELECT_MULTIPLE_ENTRIES_QUERY, (aggregate_name, tuple(ids))):
            yield SnapshotEntry(*row)
            count += 1
        if count < len(ids):
            raise Exception

    # noinspection PyUnusedLocal
    async def select(self, *args, **kwargs) -> AsyncIterator[SnapshotEntry]:
        """Select a sequence of ``MinosSnapshotEntry`` objects.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A sequence of ``MinosSnapshotEntry`` objects.
        """
        async for row in self.submit_query_and_iter(_SELECT_ALL_ENTRIES_QUERY):
            yield SnapshotEntry(*row)


_SELECT_ALL_ENTRIES_QUERY = """
SELECT aggregate_id, aggregate_name, version, data, created_at, updated_at
FROM snapshot;
""".strip()

_SELECT_MULTIPLE_ENTRIES_QUERY = """
SELECT aggregate_id, aggregate_name, version, data, created_at, updated_at
FROM snapshot
WHERE aggregate_name = %s AND aggregate_id IN %s;
""".strip()
