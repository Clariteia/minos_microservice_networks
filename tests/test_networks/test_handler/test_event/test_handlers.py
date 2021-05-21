import datetime
import unittest

import aiopg

from minos.common import (
    Event,
    MinosConfigException,
)
from minos.common.testing import PostgresAsyncTestCase
from minos.networks import (
    EventHandler,
    MinosNetworkException,
)
from tests.utils import (
    BASE_PATH,
    NaiveAggregate,
)


class TestEventDispatcher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = EventHandler.from_config(config=self.config)
        self.assertIsInstance(dispatcher, EventHandler)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            EventHandler.from_config()

    async def test_if_queue_table_exists(self):
        async with EventHandler.from_config(config=self.config):
            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "SELECT 1 "
                        "FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_name = 'event_queue';"
                    )
                    ret = []
                    async for row in cur:
                        ret.append(row)

            assert ret == [(1,)]

    async def test_get_event_handler(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TestEventQueueAdd", model=model.classname, items=[])
        m = EventHandler.from_config(config=self.config)

        cls = m.get_event_handler(topic="TicketAdded")
        result = await cls(topic="TicketAdded", event=event_instance)

        assert result == "request_added"

    async def test_non_implemented_action(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="NotExisting", model=model.classname, items=[])
        m = EventHandler.from_config(config=self.config)

        with self.assertRaises(MinosNetworkException) as context:
            cls = m.get_event_handler(topic=event_instance.topic)
            await cls(topic=event_instance.topic, event=event_instance)

        self.assertTrue(
            "topic NotExisting have no controller/action configured, please review th configuration file"
            in str(context.exception)
        )

    async def test_event_dispatch(self):
        async with EventHandler.from_config(config=self.config) as handler:
            model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
            event_instance = Event(topic="TicketAdded", model=model.classname, items=[])
            bin_data = event_instance.avro_bytes
            Event.from_avro_bytes(bin_data)

            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO event_queue (topic, partition_id, binary_data, creation_date) "
                        "VALUES (%s, %s, %s, %s) "
                        "RETURNING id;",
                        (event_instance.topic, 0, bin_data, datetime.datetime.now(),),
                    )

                    queue_id = await cur.fetchone()

            assert queue_id[0] > 0

            # Must get the record, call on_reply function and delete the record from DB
            await handler.dispatch()

            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM event_queue WHERE id=%d" % (queue_id))
                    records = await cur.fetchone()

            assert records[0] == 0

    async def test_event_dispatch_wrong_event(self):
        async with EventHandler.from_config(config=self.config) as handler:
            bin_data = bytes(b"Test")

            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO event_queue (topic, partition_id, binary_data, creation_date) "
                        "VALUES (%s, %s, %s, %s) "
                        "RETURNING id;",
                        ("TicketAdded", 0, bin_data, datetime.datetime.now(),),
                    )

                    queue_id = await cur.fetchone()
            assert queue_id[0] > 0

            # Must get the record, call on_reply function and delete the record from DB
            await handler.dispatch()

            async with aiopg.connect(**self.events_queue_db) as connect:
                async with connect.cursor() as cur:
                    await cur.execute("SELECT COUNT(*) FROM event_queue WHERE id=%d" % (queue_id))
                    records = await cur.fetchone()

            assert records[0] == 1


if __name__ == "__main__":
    unittest.main()
