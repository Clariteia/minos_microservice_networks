import datetime

import aiopg
from minos.common import (
    Command,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    MinosCommandHandlerDispatcher,
    MinosNetworkException,
)
from tests.utils import (
    BASE_PATH,
    NaiveAggregate,
)


class TestCommandDispatcher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = MinosCommandHandlerDispatcher.from_config(config=self.config)
        self.assertIsInstance(dispatcher, MinosCommandHandlerDispatcher)

    async def test_if_queue_table_exists(self):
        handler = MinosCommandHandlerDispatcher.from_config(config=self.config)
        await handler.setup()

        async with aiopg.connect(**self.commands_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "SELECT 1 "
                    "FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'command_queue';"
                )
                ret = []
                async for row in cur:
                    ret.append(row)

        assert ret == [(1,)]

    async def test_get_event_handler(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = Command(
            topic="AddOrder",
            model=model.classname,
            items=[],
            saga_id="43434jhij",
            task_id="juhjh34",
            reply_on="mkk2334",
        )
        m = MinosCommandHandlerDispatcher.from_config(config=self.config)

        cls = m.get_event_handler(topic=event_instance.topic)
        result = await cls(topic=event_instance.topic, command=event_instance)

        assert result == "add_order"

    async def test_non_implemented_action(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        instance = Command(
            topic="NotExisting",
            model=model.classname,
            items=[],
            saga_id="43434jhij",
            task_id="juhjh34",
            reply_on="mkk2334",
        )
        m = MinosCommandHandlerDispatcher.from_config(config=self.config)

        with self.assertRaises(MinosNetworkException) as context:
            cls = m.get_event_handler(topic=instance.topic)
            await cls(topic=instance.topic, command=instance)

        self.assertTrue(
            "topic NotExisting have no controller/action configured, please review th configuration file"
            in str(context.exception)
        )

    async def test_none_config(self):
        handler = MinosCommandHandlerDispatcher.from_config(config=None)

        self.assertIsNone(handler)

    async def test_event_queue_checker(self):
        handler = MinosCommandHandlerDispatcher.from_config(config=self.config)
        await handler.setup()

        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        instance = Command(
            topic="AddOrder",
            model=model.classname,
            items=[],
            saga_id="43434jhij",
            task_id="juhjh34",
            reply_on="mkk2334",
        )
        bin_data = instance.avro_bytes
        Command.from_avro_bytes(bin_data)

        async with aiopg.connect(**self.commands_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO command_queue (topic, partition_id, binary_data, creation_date) "
                    "VALUES (%s, %s, %s, %s) "
                    "RETURNING id;",
                    (instance.topic, 0, bin_data, datetime.datetime.now(),),
                )

                queue_id = await cur.fetchone()
                affected_rows = cur.rowcount

        assert affected_rows == 1
        assert queue_id[0] > 0

        # Must get the record, call on_reply function and delete the record from DB
        await handler.queue_checker()

        async with aiopg.connect(**self.commands_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM command_queue WHERE id=%d" % (queue_id))
                records = await cur.fetchone()

        assert records[0] == 0

    async def test_event_queue_checker_wrong_event(self):
        handler = MinosCommandHandlerDispatcher.from_config(config=self.config)
        await handler.setup()
        bin_data = bytes(b"Test")

        async with aiopg.connect(**self.commands_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO command_queue (topic, partition_id, binary_data, creation_date) "
                    "VALUES (%s, %s, %s, %s) "
                    "RETURNING id;",
                    ("AddOrder", 0, bin_data, datetime.datetime.now(),),
                )

                queue_id = await cur.fetchone()
                affected_rows = cur.rowcount

        assert affected_rows == 1
        assert queue_id[0] > 0

        # Must get the record, call on_reply function and delete the record from DB
        await handler.queue_checker()

        async with aiopg.connect(**self.commands_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM command_queue WHERE id=%d" % (queue_id))
                records = await cur.fetchone()

        assert records[0] == 1
