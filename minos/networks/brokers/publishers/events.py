from __future__ import (
    annotations,
)

import logging

from minos.aggregate import (
    AggregateDiff,
)
from minos.common import (
    MinosConfig,
)

from ..messages import (
    Event,
)
from .abc import (
    Broker,
)

logger = logging.getLogger(__name__)


class EventBroker(Broker):
    """Minos Event broker class."""

    ACTION = "event"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventBroker:
        return cls(*args, **config.broker.queue._asdict(), **kwargs)

    # noinspection PyMethodOverriding
    async def send(self, data: AggregateDiff, topic: str, **kwargs) -> int:
        """Send an ``Event``.

        :param data: The data to be send.
        :param topic: Topic in which the message will be published.
        :return: This method does not return anything.
        """
        event = Event(topic, data)
        logger.info(f"Sending '{event!s}'...")
        return await self.enqueue(event.topic, event.avro_bytes)
