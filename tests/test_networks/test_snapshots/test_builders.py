"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from datetime import (
    datetime,
)
from unittest.mock import (
    MagicMock,
    call,
    patch,
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
)


class TestSnapshotBuilder(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_type(self):
        self.assertTrue(issubclass(SnapshotBuilder, SnapshotSetup))

    def test_from_config(self):
        dispatcher = SnapshotBuilder.from_config(config=self.config)
        self.assertEqual(self.config.snapshot.host, dispatcher.host)
        self.assertEqual(self.config.snapshot.port, dispatcher.port)
        self.assertEqual(self.config.snapshot.database, dispatcher.database)
        self.assertEqual(self.config.snapshot.user, dispatcher.user)
        self.assertEqual(self.config.snapshot.password, dispatcher.password)
        self.assertEqual(self.config.repository.host, dispatcher.repository.host)
        self.assertEqual(self.config.repository.port, dispatcher.repository.port)
        self.assertEqual(self.config.repository.database, dispatcher.repository.database)
        self.assertEqual(self.config.repository.user, dispatcher.repository.user)
        self.assertEqual(self.config.repository.password, dispatcher.repository.password)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            SnapshotBuilder.from_config()

    async def test_dispatch_select(self):
        await self._populate()
        async with SnapshotBuilder.from_config(config=self.config) as dispatcher:
            await dispatcher.dispatch()

        async with SnapshotReader.from_config(config=self.config) as snapshot:
            observed = [v async for v in snapshot.select()]

        expected = [
            SnapshotEntry.from_aggregate(Car(2, 2, 3, "blue")),
            SnapshotEntry.from_aggregate(Car(3, 1, 3, "blue")),
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    async def test_dispatch_ignore_previous_version(self):

        dispatcher = SnapshotBuilder.from_config(config=self.config)
        await dispatcher.setup()

        car = Car(1, 1, 3, "blue")
        # noinspection PyTypeChecker
        aggregate_name: str = car.classname

        async def _fn(*args, **kwargs):
            yield MinosRepositoryEntry(1, aggregate_name, 1, car.avro_bytes)
            yield MinosRepositoryEntry(1, aggregate_name, 3, car.avro_bytes)
            yield MinosRepositoryEntry(1, aggregate_name, 2, car.avro_bytes)

        with patch("minos.common.PostgreSqlMinosRepository.select", _fn):
            await dispatcher.dispatch()

        async with SnapshotReader.from_config(config=self.config) as snapshot:
            observed = [v async for v in snapshot.select()]

        expected = [SnapshotEntry(1, aggregate_name, 3, car.avro_bytes)]
        self._assert_equal_snapshot_entries(expected, observed)

    def _assert_equal_snapshot_entries(self, expected: list[SnapshotEntry], observed: list[SnapshotEntry]):
        self.assertEqual(len(expected), len(observed))
        for exp, obs in zip(expected, observed):
            self.assertEqual(exp.aggregate, obs.aggregate)
            self.assertIsInstance(obs.created_at, datetime)
            self.assertIsInstance(obs.updated_at, datetime)

    async def test_dispatch_with_offset(self):
        async with await self._populate() as repository:
            async with SnapshotBuilder.from_config(config=self.config) as dispatcher:
                mock = MagicMock(side_effect=dispatcher.repository.select)
                dispatcher.repository.select = mock

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_ge=0), mock.call_args)
                mock.reset_mock()

                # noinspection PyTypeChecker
                await repository.insert(MinosRepositoryEntry(3, Car.classname, 1, Car(1, 1, 3, "blue").avro_bytes))

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_ge=7), mock.call_args)
                mock.reset_mock()

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_ge=8), mock.call_args)
                mock.reset_mock()

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_ge=8), mock.call_args)
                mock.reset_mock()

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
            return repository


if __name__ == "__main__":
    unittest.main()
