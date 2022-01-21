import unittest
from collections import namedtuple
from unittest.mock import (
    AsyncMock,
    MagicMock,
)

from aiokafka import AIOKafkaConsumer
from kafka import KafkaAdminClient

from minos.common import MinosConfig
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriber,
    KafkaBrokerSubscriber,
)
from tests.utils import CONFIG_FILE_PATH

_ConsumerMessage = namedtuple("_ConsumerMessage", ["value"])


class TestKafkaBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(KafkaBrokerSubscriber, BrokerSubscriber))

    def test_from_config(self):
        config = MinosConfig(CONFIG_FILE_PATH)
        subscriber = KafkaBrokerSubscriber.from_config(config, topics={"foo", "bar"})
        self.assertEqual(config.broker.host, subscriber.broker_host)
        self.assertEqual(config.broker.port, subscriber.broker_port)
        self.assertEqual(config.service.name, subscriber.group_id)
        self.assertEqual(False, subscriber.remove_topics_on_destroy)
        self.assertEqual({"foo", "bar"}, subscriber.topics)

    def test_from_config_none_group_id(self):
        subscriber = KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"}, group_id=None)
        self.assertEqual(None, subscriber.group_id)

    async def test_client(self):
        subscriber = KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"})

        client = subscriber.client
        self.assertIsInstance(client, AIOKafkaConsumer)

    def test_admin_client(self):
        subscriber = KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"})

        client = subscriber.admin_client
        self.assertIsInstance(client, KafkaAdminClient)

    async def test_setup_destroy_client(self):
        subscriber = KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"})

        start_mock = AsyncMock()
        stop_mock = AsyncMock()
        subscriber.client.start = start_mock
        subscriber.client.stop = stop_mock

        admin_stop_mock = MagicMock()
        admin_create_topic_mock = MagicMock()
        admin_remove_topic_mock = MagicMock()
        subscriber.admin_client.stop = admin_stop_mock
        subscriber.admin_client.create_topics = admin_create_topic_mock
        subscriber.admin_client.delete_topics = admin_remove_topic_mock

        async with subscriber:
            self.assertEqual(1, start_mock.call_count)
            self.assertEqual(0, stop_mock.call_count)

            start_mock.reset_mock()
            stop_mock.reset_mock()

        self.assertEqual(0, start_mock.call_count)
        self.assertEqual(1, stop_mock.call_count)

    async def test_setup_destroy_admin_client(self):
        subscriber = KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"})

        start_mock = AsyncMock()
        stop_mock = AsyncMock()
        subscriber.client.start = start_mock
        subscriber.client.stop = stop_mock

        admin_stop_mock = MagicMock()
        admin_create_topic_mock = MagicMock()
        admin_remove_topic_mock = MagicMock()
        subscriber.admin_client.stop = admin_stop_mock
        subscriber.admin_client.create_topics = admin_create_topic_mock
        subscriber.admin_client.delete_topics = admin_remove_topic_mock

        async with subscriber:
            self.assertEqual(0, admin_stop_mock.call_count)
            self.assertEqual(1, admin_create_topic_mock.call_count)

            observed = admin_create_topic_mock.call_args.args[0]
            self.assertIsInstance(observed, list)
            self.assertEqual(2, len(observed))
            self.assertEqual({"foo", "bar"}, set(o.name for o in observed))

            self.assertEqual(0, admin_remove_topic_mock.call_count)

            admin_stop_mock.reset_mock()
            admin_create_topic_mock.reset_mock()
            admin_remove_topic_mock.reset_mock()

        self.assertEqual(0, admin_stop_mock.call_count)
        self.assertEqual(0, admin_create_topic_mock.call_count)
        self.assertEqual(0, admin_remove_topic_mock.call_count)

    async def test_setup_destroy_admin_client_removing_topics(self):
        subscriber = KafkaBrokerSubscriber.from_config(
            CONFIG_FILE_PATH, topics={"foo", "bar"}, remove_topics_on_destroy=True
        )

        start_mock = AsyncMock()
        stop_mock = AsyncMock()
        subscriber.client.start = start_mock
        subscriber.client.stop = stop_mock

        admin_stop_mock = MagicMock()
        admin_create_topic_mock = MagicMock()
        admin_remove_topic_mock = MagicMock()
        subscriber.admin_client.stop = admin_stop_mock
        subscriber.admin_client.create_topics = admin_create_topic_mock
        subscriber.admin_client.delete_topics = admin_remove_topic_mock

        async with subscriber:
            self.assertEqual(0, admin_stop_mock.call_count)
            self.assertEqual(1, admin_create_topic_mock.call_count)
            self.assertEqual(0, admin_remove_topic_mock.call_count)

            admin_stop_mock.reset_mock()
            admin_create_topic_mock.reset_mock()
            admin_remove_topic_mock.reset_mock()

        self.assertEqual(0, admin_stop_mock.call_count)
        self.assertEqual(0, admin_create_topic_mock.call_count)
        self.assertEqual(1, admin_remove_topic_mock.call_count)

        observed = admin_remove_topic_mock.call_args.args[0]
        self.assertIsInstance(observed, list)
        self.assertEqual(2, len(observed))

        self.assertEqual({"foo", "bar"}, set(observed))

    async def test_receive(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        async with KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"}) as subscriber:
            get_mock = AsyncMock(side_effect=[_ConsumerMessage(m.avro_bytes) for m in messages])
            subscriber.client.getone = get_mock

            self.assertEqual(messages[0], await subscriber.receive())
            self.assertEqual(messages[1], await subscriber.receive())


if __name__ == "__main__":
    unittest.main()
