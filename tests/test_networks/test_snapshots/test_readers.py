"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import sys
import unittest
from datetime import (
    datetime,
)

from dependency_injector import (
    containers,
    providers,
)

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
    SnapshotSetup,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
    FakeRepository,
    FakeSnapshot,
)


class TestSnapshotReader(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.container = containers.DynamicContainer()
        self.container.event_broker = providers.Singleton(FakeBroker)
        self.container.repository = providers.Singleton(FakeRepository)
        self.container.snapshot = providers.Singleton(FakeSnapshot)
        self.container.wire(modules=[sys.modules[__name__]])

    def tearDown(self) -> None:
        self.container.unwire()
        super().tearDown()

    def test_type(self):
        self.assertTrue(issubclass(SnapshotReader, SnapshotSetup))

    def test_from_config(self):
        reader = SnapshotReader.from_config(config=self.config)
        self.assertEqual(self.config.snapshot.host, reader.host)
        self.assertEqual(self.config.snapshot.port, reader.port)
        self.assertEqual(self.config.snapshot.database, reader.database)
        self.assertEqual(self.config.snapshot.user, reader.user)
        self.assertEqual(self.config.snapshot.password, reader.password)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            SnapshotReader.from_config()

    async def test_get(self):
        await self._populate()
        async with SnapshotReader.from_config(config=self.config) as snapshot:
            observed = [v async for v in snapshot.get("tests.aggregate_classes.Car", [2, 3])]

        expected = [Car(2, 2, 3, "blue"), Car(3, 1, 3, "blue")]
        self.assertEqual(expected, observed)

    async def test_get_raises(self):
        await self._populate()
        async with SnapshotReader.from_config(config=self.config) as snapshot:
            with self.assertRaises(Exception):
                # noinspection PyStatementEffect
                [v async for v in snapshot.get("tests.aggregate_classes.Car", [1, 2, 3])]

    async def test_select(self):
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
