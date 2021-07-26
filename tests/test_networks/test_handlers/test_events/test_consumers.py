"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    EventConsumer,
)
from tests.utils import (
    BASE_PATH,
)


class TestEventConsumer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        consumer = EventConsumer.from_config(config=self.config)
        self.assertIsInstance(consumer, EventConsumer)
        self.assertEqual({"TicketAdded", "TicketDeleted"}, consumer.topics)
        self.assertEqual(self.config.events.broker, consumer._broker)
        self.assertEqual(self.config.events.queue.host, consumer.host)
        self.assertEqual(self.config.events.queue.port, consumer.port)
        self.assertEqual(self.config.events.queue.database, consumer.database)
        self.assertEqual(self.config.events.queue.user, consumer.user)
        self.assertEqual(self.config.events.queue.password, consumer.password)

    def test_table_name(self):
        self.assertEqual("event_queue", EventConsumer.TABLE_NAME)


if __name__ == "__main__":
    unittest.main()
