from __future__ import (
    annotations,
)

import logging
from contextvars import (
    Token,
)
from itertools import (
    count,
)
from time import (
    sleep,
)
from typing import (
    AsyncContextManager,
    Optional,
)
from uuid import (
    uuid4,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)
from kafka import (
    KafkaAdminClient,
)
from kafka.admin import (
    NewTopic,
)
from kafka.errors import (
    NoBrokersAvailable,
)

from minos.common import (
    MinosConfig,
    MinosPool,
    NotProvidedException,
)

from ..handlers import (
    BrokerConsumer,
)
from ..messages import (
    REQUEST_REPLY_TOPIC_CONTEXT_VAR,
)
from ..publishers import (
    BrokerPublisher,
)
from .brokers import (
    DynamicBroker,
)

logger = logging.getLogger(__name__)


class DynamicBrokerPool(MinosPool):
    """Dynamic Broker Pool class."""

    def __init__(
        self,
        config: MinosConfig,
        client: KafkaAdminClient,
        consumer: BrokerConsumer,
        publisher: BrokerPublisher,
        maxsize: int = 5,
        recycle: Optional[int] = 3600,
        *args,
        **kwargs,
    ):
        super().__init__(maxsize=maxsize, recycle=recycle, *args, **kwargs)
        self.config = config
        self.client = client
        self.consumer = consumer
        self.publisher = publisher

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> DynamicBrokerPool:
        kwargs["client"] = cls._get_client(config)
        kwargs["consumer"] = cls._get_consumer(**kwargs)
        kwargs["publisher"] = cls._get_publisher(**kwargs)
        return cls(config, **kwargs)

    @staticmethod
    def _get_client(config: MinosConfig, max_attempts: int = 5, delay: float = 1):
        for attempt in count(start=1):
            try:
                return KafkaAdminClient(bootstrap_servers=f"{config.broker.host}:{config.broker.port}")
            except NoBrokersAvailable as exc:
                if attempt >= max_attempts:
                    raise exc
                logger.warning("Waiting for the broker to be available...")
                sleep(delay)

    # noinspection PyUnusedLocal
    @staticmethod
    @inject
    def _get_consumer(
        consumer: Optional[BrokerConsumer] = None,
        broker_consumer: BrokerConsumer = Provide["broker_consumer"],
        **kwargs,
    ) -> BrokerConsumer:
        if consumer is None:
            consumer = broker_consumer
        if consumer is None or isinstance(consumer, Provide):
            raise NotProvidedException(f"A {BrokerConsumer!r} object must be provided.")
        return consumer

    # noinspection PyUnusedLocal
    @staticmethod
    @inject
    def _get_publisher(
        publisher: Optional[BrokerPublisher] = None,
        broker_publisher: BrokerPublisher = Provide["broker_publisher"],
        **kwargs,
    ) -> BrokerPublisher:
        if publisher is None:
            publisher = broker_publisher
        if publisher is None or isinstance(publisher, Provide):
            raise NotProvidedException(f"A {BrokerPublisher!r} object must be provided.")
        return publisher

    async def _destroy(self) -> None:
        await super()._destroy()
        self.client.close()

    async def _create_instance(self) -> DynamicBroker:
        topic = str(uuid4()).replace("-", "")
        await self._create_reply_topic(topic)
        await self._subscribe_reply_topic(topic)
        instance = DynamicBroker.from_config(self.config, topic=topic, publisher=self.publisher)
        await instance.setup()
        return instance

    async def _destroy_instance(self, instance: DynamicBroker):
        await instance.destroy()
        await self._unsubscribe_reply_topic(instance.topic)
        await self._delete_reply_topic(instance.topic)

    async def _create_reply_topic(self, topic: str) -> None:
        logger.info(f"Creating {topic!r} topic...")
        self.client.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])

    async def _delete_reply_topic(self, topic: str) -> None:
        logger.info(f"Deleting {topic!r} topic...")
        self.client.delete_topics([topic])

    async def _subscribe_reply_topic(self, topic: str) -> None:
        await self.consumer.add_topic(topic)

    async def _unsubscribe_reply_topic(self, topic: str) -> None:
        await self.consumer.remove_topic(topic)

    def acquire(self, *args, **kwargs) -> AsyncContextManager:
        """Acquire a new instance wrapped on an asynchronous context manager.

        :return: An asynchronous context manager.
        """
        return _ReplyTopicContextManager(super().acquire())


class _ReplyTopicContextManager:
    _token: Optional[Token]

    def __init__(self, wrapper: AsyncContextManager[DynamicBroker]):
        self.wrapper = wrapper
        self._token = None

    async def __aenter__(self) -> DynamicBroker:
        handler = await self.wrapper.__aenter__()
        self._token = REQUEST_REPLY_TOPIC_CONTEXT_VAR.set(handler.topic)
        return handler

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        REQUEST_REPLY_TOPIC_CONTEXT_VAR.reset(self._token)
        await self.wrapper.__aexit__(exc_type, exc_val, exc_tb)
