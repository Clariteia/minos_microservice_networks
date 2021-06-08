"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from datetime import (
    datetime,
)

import aiopg

from minos.common import (
    MinosConfigException,
    MinosRepositoryEntry,
    PostgreSqlMinosRepository,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    SnapshotBuilder,
    SnapshotEntry,
    SnapshotReader,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    BASE_PATH,
)


class TestSnapshotReader(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = SnapshotReader.from_config(config=self.config)
        self.assertEqual(self.config.snapshot.host, dispatcher.host)
        self.assertEqual(self.config.snapshot.port, dispatcher.port)
        self.assertEqual(self.config.snapshot.database, dispatcher.database)
        self.assertEqual(self.config.snapshot.user, dispatcher.user)
        self.assertEqual(self.config.snapshot.password, dispatcher.password)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            SnapshotReader.from_config()

    async def test_setup_snapshot_table(self):
        async with SnapshotReader.from_config(config=self.config):
            async with aiopg.connect(**self.snapshot_db) as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT EXISTS (SELECT FROM pg_tables "
                        "WHERE schemaname = 'public' AND tablename = 'snapshot');"
                    )
                    observed = (await cursor.fetchone())[0]
        self.assertEqual(True, observed)

    async def test_setup_snapshot_aux_offset_table(self):
        async with SnapshotReader.from_config(config=self.config):
            async with aiopg.connect(**self.snapshot_db) as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT EXISTS (SELECT FROM pg_tables WHERE "
                        "schemaname = 'public' AND tablename = 'snapshot_aux_offset');"
                    )
                    observed = (await cursor.fetchone())[0]
        self.assertEqual(True, observed)

    async def test_dispatch_select(self):
        await self._populate()

        async with SnapshotReader.from_config(config=self.config) as snapshot:
            observed = [v async for v in snapshot.select()]

        expected = [
            SnapshotEntry.from_aggregate(Car(2, 2, 3, "blue")),
            SnapshotEntry.from_aggregate(Car(3, 1, 3, "blue")),
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    def _assert_equal_snapshot_entries(self, expected: list[SnapshotEntry], observed: list[SnapshotEntry]):
        self.assertEqual(len(expected), len(observed))
        for exp, obs in zip(expected, observed):
            self.assertEqual(exp.aggregate, obs.aggregate)
            self.assertIsInstance(obs.created_at, datetime)
            self.assertIsInstance(obs.updated_at, datetime)

    async def _populate(self):
        car = Car(1, 1, 3, "blue")
        # noinspection PyTypeChecker
        aggregate_name: str = car.classname
        async with PostgreSqlMinosRepository.from_config(config=self.config) as repository:
            await repository.insert(MinosRepositoryEntry(1, aggregate_name, 1, car.avro_bytes))
            await repository.update(MinosRepositoryEntry(1, aggregate_name, 2, car.avro_bytes))
            await repository.insert(MinosRepositoryEntry(2, aggregate_name, 1, car.avro_bytes))
            await repository.update(MinosRepositoryEntry(1, aggregate_name, 3, car.avro_bytes))
            await repository.delete(MinosRepositoryEntry(1, aggregate_name, 4))
            await repository.update(MinosRepositoryEntry(2, aggregate_name, 2, car.avro_bytes))
            await repository.insert(MinosRepositoryEntry(3, aggregate_name, 1, car.avro_bytes))
        async with SnapshotBuilder.from_config(config=self.config) as dispatcher:
            await dispatcher.dispatch()


if __name__ == "__main__":
    unittest.main()
