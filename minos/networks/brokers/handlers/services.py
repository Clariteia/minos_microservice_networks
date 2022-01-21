import logging
from abc import abstractmethod

from aiomisc import Service
from cached_property import cached_property

from ..handlers import (
    InMemoryQueuedKafkaBrokerHandler,
    KafkaBrokerHandler,
    PostgreSqlQueuedKafkaBrokerHandler,
)
from .impl import BrokerHandler

logger = logging.getLogger(__name__)


class BrokerHandlerService(Service):
    """Broker Handler Service class."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_kwargs = kwargs

    async def start(self) -> None:
        """Start the service execution.

        :return: This method does not return anything.
        """
        await self.handler.setup()

        try:
            self.start_event.set()
        except RuntimeError:
            logger.warning("Runtime is not properly setup.")

        await self.handler.run()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await self.handler.destroy()

    @cached_property
    def handler(self) -> BrokerHandler:
        """Get the service handler.

        :return: A ``Handler`` instance.
        """
        return self._handler_cls().from_config(**self._init_kwargs)

    @staticmethod
    @abstractmethod
    def _handler_cls() -> type[BrokerHandler]:
        """TODO"""
        raise NotImplementedError


class KafkaBrokerHandlerService(BrokerHandlerService):
    """TODO"""

    @staticmethod
    def _handler_cls() -> type[BrokerHandler]:
        return KafkaBrokerHandler


class InMemoryQueuedKafkaBrokerHandlerService(BrokerHandlerService):
    """TODO"""

    @staticmethod
    def _handler_cls() -> type[BrokerHandler]:
        return InMemoryQueuedKafkaBrokerHandler


class PostgreSqlQueuedKafkaBrokerHandlerService(BrokerHandlerService):
    """TODO"""

    @staticmethod
    def _handler_cls() -> type[BrokerHandler]:
        return PostgreSqlQueuedKafkaBrokerHandler
