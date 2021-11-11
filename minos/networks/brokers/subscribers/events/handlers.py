from __future__ import (
    annotations,
)

import logging
from asyncio import (
    gather,
)
from collections import (
    defaultdict,
)
from inspect import (
    isawaitable,
)
from operator import (
    attrgetter,
)
from typing import (
    Awaitable,
    Callable,
    Optional,
)

from minos.common import (
    MinosConfig,
)

from ....decorators import (
    EnrouteBuilder,
)
from ....messages import (
    ResponseException,
)
from ...messages import (
    Event,
)
from ..abc import (
    Handler,
)
from ..entries import (
    HandlerEntry,
)
from ..messages import (
    HandlerRequest,
)

logger = logging.getLogger(__name__)

uuid_getter = attrgetter("data.data.uuid")
version_getter = attrgetter("data.data.version")


class EventHandler(Handler):
    """Event Handler class."""

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventHandler:
        handlers = cls._handlers_from_config(config, **kwargs)
        # noinspection PyProtectedMember
        return cls(handlers=handlers, **config.broker.queue._asdict(), **kwargs)

    @staticmethod
    def _handlers_from_config(config: MinosConfig, **kwargs) -> dict[str, Callable[[HandlerRequest], Awaitable]]:
        builder = EnrouteBuilder(*config.services)
        handlers = builder.get_broker_event(config=config, **kwargs)
        handlers = {decorator.topic: fn for decorator, fn in handlers.items()}
        return handlers

    async def _dispatch_entries(self, entries: list[HandlerEntry]) -> None:
        grouped = defaultdict(list)
        for entry in entries:
            grouped[uuid_getter(entry)].append(entry)

        for group in grouped.values():
            group.sort(key=version_getter)

        futures = (self._dispatch_group(group) for group in grouped.values())
        await gather(*futures)

    async def _dispatch_group(self, entries: list[HandlerEntry]):
        for entry in entries:
            await self._dispatch_one(entry)

    async def dispatch_one(self, entry: HandlerEntry) -> None:
        """Dispatch one row.

        :param entry: Entry to be dispatched.
        :return: This method does not return anything.
        """
        logger.info(f"Dispatching '{entry!s}'...")

        fn = self.get_callback(entry.callback)
        await fn(entry.data)

    @staticmethod
    def get_callback(fn: Callable[[HandlerRequest], Optional[Awaitable[None]]]) -> Callable[[Event], Awaitable[None]]:
        """Get the handler function to be used by the Event Handler.

        :param fn: The action function.
        :return: A wrapper function around the given one that is compatible with the Event Handler API.
        """

        async def _fn(raw: Event) -> None:
            try:
                request = HandlerRequest(raw)
                response = fn(request)
                if isawaitable(response):
                    await response
            except ResponseException as exc:
                logger.warning(f"Raised an application exception: {exc!s}")
            except Exception as exc:
                logger.exception(f"Raised a system exception: {exc!r}")

        return _fn
