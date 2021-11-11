from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from minos.common import MinosConfig

from ..messages import (
    CommandReply,
    CommandStatus,
)
from .abc import Broker

logger = logging.getLogger(__name__)


class CommandReplyBroker(Broker):
    """Minos Command Broker Class."""

    ACTION = "commandReply"

    def __init__(self, *args, service_name: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.service_name = service_name

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandReplyBroker:
        return cls(*args, service_name=config.service.name, **config.broker.queue._asdict(), **kwargs)

    # noinspection PyMethodOverriding
    async def send(self, data: Any, topic: str, saga: UUID, status: CommandStatus, **kwargs) -> int:
        """Send a ``CommandReply``.

        :param data: The data to be send.
        :param topic: Topic in which the message will be published.
        :param saga: Saga identifier.
        :param status: command status.
        :return: This method does not return anything.
        """

        command_reply = CommandReply(topic, data, saga=saga, status=status, service_name=self.service_name)
        logger.info(f"Sending '{command_reply!s}'...")
        return await self.enqueue(command_reply.topic, command_reply.avro_bytes)
