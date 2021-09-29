import unittest
from unittest.mock import (
    AsyncMock,
)

from aiomisc import (
    Service,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    TaskScheduler,
    TaskSchedulerService,
)
from tests.utils import (
    BASE_PATH,
)


class TestTaskSchedulerService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_is_instance(self):
        service = TaskSchedulerService(config=self.config)
        self.assertIsInstance(service, Service)

    def test_dispatcher(self):
        service = TaskSchedulerService(config=self.config)
        self.assertIsInstance(service.scheduler, TaskScheduler)

    async def test_start_stop(self):
        service = TaskSchedulerService(config=self.config)

        setup_mock = AsyncMock()
        destroy_mock = AsyncMock()
        start = AsyncMock()

        service.scheduler.setup = setup_mock
        service.scheduler.destroy = destroy_mock
        service.scheduler.start = start

        await service.start()

        self.assertEqual(1, setup_mock.call_count)
        self.assertEqual(1, start.call_count)
        self.assertEqual(0, destroy_mock.call_count)

        setup_mock.reset_mock()
        destroy_mock.reset_mock()
        start.reset_mock()

        await service.stop()

        self.assertEqual(0, setup_mock.call_count)
        self.assertEqual(0, start.call_count)
        self.assertEqual(1, destroy_mock.call_count)


if __name__ == "__main__":
    unittest.main()