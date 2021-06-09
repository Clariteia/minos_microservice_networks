"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import sys
import unittest

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    MinosException,
)
from minos.networks import (
    MinosNetworkException,
    MinosPreviousVersionSnapshotException,
    MinosSnapshotException,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    FakeBroker,
    FakeRepository,
    FakeSnapshot,
)


class TestExceptions(unittest.TestCase):
    def setUp(self) -> None:
        self.container = containers.DynamicContainer()
        self.container.event_broker = providers.Singleton(FakeBroker)
        self.container.repository = providers.Singleton(FakeRepository)
        self.container.snapshot = providers.Singleton(FakeSnapshot)
        self.container.wire(modules=[sys.modules[__name__]])

    def tearDown(self) -> None:
        self.container.unwire()

    def test_type(self):
        self.assertTrue(issubclass(MinosNetworkException, MinosException))

    def test_snapshot(self):
        self.assertTrue(issubclass(MinosSnapshotException, MinosNetworkException))

    def test_snapshot_previous_version(self):
        self.assertTrue(issubclass(MinosPreviousVersionSnapshotException, MinosSnapshotException))

    def test_snapshot_previous_version_repr(self):
        previous = Car(1, 2, 3, "blue")
        new = Car(1, 1, 5, "blue")
        exception = MinosPreviousVersionSnapshotException(previous, new)
        expected = (
            "MinosPreviousVersionSnapshotException(message=\"Version for 'tests.aggregate_classes.Car' "
            'aggregate must be greater than 2. Obtained: 1")'
        )
        self.assertEqual(expected, repr(exception))


if __name__ == "__main__":
    unittest.main()
