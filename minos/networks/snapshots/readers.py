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
    MinosConfig,
)

from .abc import (
    SnapshotSetup,
)
from .entries import (
    SnapshotEntry,
)


class SnapshotReader(SnapshotSetup):
    """Minos Snapshot Reader class."""

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> SnapshotReader:
        return cls(*args, **config.snapshot._asdict(), **kwargs)

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

_SELECT_OFFSET_QUERY = """
SELECT value
FROM snapshot_aux_offset
WHERE id = TRUE;
"""
