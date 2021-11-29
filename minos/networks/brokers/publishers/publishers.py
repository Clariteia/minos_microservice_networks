from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
)
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from psycopg2.sql import (
    SQL,
)

from minos.common import (
    MinosConfig,
)

from ..messages import (
    TRACE_CONTEXT_VAR,
    BrokerMessage,
    BrokerMessageStatus,
    BrokerMessageStrategy,
    TraceStep,
)
from .abc import (
    BrokerPublisherSetup,
)

logger = logging.getLogger(__name__)


class BrokerPublisher(BrokerPublisherSetup, ABC):
    """Broker Publisher class."""

    def __init__(self, *args, service_name: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.service_name = service_name

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> BrokerPublisher:
        # noinspection PyProtectedMember
        return cls(*args, service_name=config.service.name, **config.broker.queue._asdict(), **kwargs)

    # noinspection PyMethodOverriding
    async def send(
        self,
        data: Any,
        topic: str,
        *,
        identifier: Optional[UUID] = None,
        reply_topic: Optional[str] = None,
        user: Optional[UUID] = None,
        status: BrokerMessageStatus = BrokerMessageStatus.SUCCESS,
        strategy: BrokerMessageStrategy = BrokerMessageStrategy.UNICAST,
        trace: Optional[list[TraceStep]] = None,
        headers: Optional[dict[str, str]] = None,
        **kwargs,
    ) -> int:
        """Send a ``BrokerMessage``.

        :param data: The data to be send.
        :param topic: Topic in which the message will be published.
        :param identifier: Saga identifier.
        :param reply_topic: An optional topic name to wait for a response.
        :param user: The user identifier that send the message.
        :param status: The status code of the message.
        :param strategy: The publishing strategy.
        :param trace: TODO
        :param headers: TODO
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        if trace is None:
            trace = (TRACE_CONTEXT_VAR.get() or list()).copy()
        trace.append(TraceStep(identifier or uuid4(), self.service_name))

        message = BrokerMessage(
            topic=topic,
            data=data,
            trace=trace,
            status=status,
            reply_topic=reply_topic,
            user=user,
            strategy=strategy,
            headers=headers,
        )
        logger.info(f"Publishing '{message!s}'...")
        return await self.enqueue(message.topic, message.strategy, message.avro_bytes)

    async def enqueue(self, topic: str, strategy: BrokerMessageStrategy, raw: bytes) -> int:
        """Send a sequence of bytes to the given topic.

        :param topic: Topic in which the bytes will be send.
        :param strategy: The publishing strategy.
        :param raw: Bytes sequence to be send.
        :return: The identifier of the message in the queue.
        """
        params = (topic, raw, strategy)
        raw = await self.submit_query_and_fetchone(_INSERT_ENTRY_QUERY, params)
        await self.submit_query(_NOTIFY_QUERY)
        return raw[0]


_INSERT_ENTRY_QUERY = SQL("INSERT INTO producer_queue (topic, data, strategy) VALUES (%s, %s, %s) RETURNING id")

_NOTIFY_QUERY = SQL("NOTIFY producer_queue")
