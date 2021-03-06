from __future__ import (
    annotations,
)

import logging
from asyncio import (
    TimeoutError,
    wait_for,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from aiopg import (
    Cursor,
)
from cached_property import (
    cached_property,
)
from dependency_injector.wiring import (
    Provide,
    inject,
)
from psycopg2.sql import (
    SQL,
    Identifier,
)

from minos.common import (
    MinosConfig,
    NotProvidedException,
)

from ...exceptions import (
    MinosHandlerNotFoundEnoughEntriesException,
)
from ...utils import (
    consume_queue,
)
from ..handlers import (
    BrokerHandlerEntry,
    BrokerHandlerSetup,
)
from ..publishers import (
    BrokerPublisher,
)

logger = logging.getLogger(__name__)


class DynamicBroker(BrokerHandlerSetup):
    """Dynamic Broker class."""

    def __init__(self, topic: str, publisher: BrokerPublisher, **kwargs):
        super().__init__(**kwargs)

        self.topic = topic
        self.publisher = publisher

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> DynamicBroker:
        kwargs["publisher"] = cls._get_publisher(**kwargs)
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

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

    async def _setup(self) -> None:
        await super()._setup()

    async def _destroy(self) -> None:
        await super()._destroy()

    # noinspection PyUnusedLocal
    async def send(self, *args, reply_topic: None = None, **kwargs) -> UUID:
        """Send a ``BrokerMessage``.

        :param args: Additional positional arguments.
        :param reply_topic: This argument is ignored if ignored in favor of ``self.topic``.
        :param kwargs: Additional named arguments.
        :return: The ``UUID`` identifier of the message.
        """
        return await self.publisher.send(*args, reply_topic=self.topic, **kwargs)

    async def get_one(self, *args, **kwargs) -> BrokerHandlerEntry:
        """Get one handler entry from the given topics.

        :param args: Additional positional parameters to be passed to get_many.
        :param kwargs: Additional named parameters to be passed to get_many.
        :return: A ``HandlerEntry`` instance.
        """
        return (await self.get_many(*args, **(kwargs | {"count": 1})))[0]

    async def get_many(self, count: int, timeout: float = 60, **kwargs) -> list[BrokerHandlerEntry]:
        """Get multiple handler entries from the given topics.

        :param timeout: Maximum time in seconds to wait for messages.
        :param count: Number of entries to be collected.
        :return: A list of ``HandlerEntry`` instances.
        """
        try:
            entries = await wait_for(self._get_many(count, **kwargs), timeout=timeout)
        except TimeoutError:
            raise MinosHandlerNotFoundEnoughEntriesException(
                f"Timeout exceeded while trying to fetch {count!r} entries from {self.topic!r}."
            )

        logger.info(f"Dispatching '{entries if count > 1 else entries[0]!s}'...")

        return entries

    async def _get_many(self, count: int, max_wait: Optional[float] = 10.0) -> list[BrokerHandlerEntry]:
        result = list()
        async with self.cursor() as cursor:

            await cursor.execute(self._queries["listen"])
            try:
                while len(result) < count:
                    await self._wait_for_entries(cursor, count - len(result), max_wait)
                    result += await self._get_entries(cursor, count - len(result))
            finally:
                await cursor.execute(self._queries["unlisten"])

        return result

    async def _wait_for_entries(self, cursor: Cursor, count: int, max_wait: Optional[float]) -> None:
        if await self._get_count(cursor):
            return

        while True:
            try:
                return await wait_for(consume_queue(cursor.connection.notifies, count), max_wait)
            except TimeoutError:
                if await self._get_count(cursor):
                    return

    async def _get_count(self, cursor) -> int:
        await cursor.execute(self._queries["count_not_processed"], (self.topic,))
        count = (await cursor.fetchone())[0]
        return count

    async def _get_entries(self, cursor: Cursor, count: int) -> list[BrokerHandlerEntry]:
        entries = list()
        async with cursor.begin():
            await cursor.execute(self._queries["select_not_processed"], (self.topic, count))
            for entry in self._build_entries(await cursor.fetchall()):
                await cursor.execute(self._queries["delete_processed"], (entry.id,))
                entries.append(entry)
        return entries

    @cached_property
    def _queries(self) -> dict[str, str]:
        # noinspection PyTypeChecker
        return {
            "listen": _LISTEN_QUERY.format(Identifier(self.topic)),
            "unlisten": _UNLISTEN_QUERY.format(Identifier(self.topic)),
            "count_not_processed": _COUNT_NOT_PROCESSED_QUERY,
            "select_not_processed": _SELECT_NOT_PROCESSED_ROWS_QUERY,
            "delete_processed": _DELETE_PROCESSED_QUERY,
        }

    @staticmethod
    def _build_entries(rows: list[tuple]) -> list[BrokerHandlerEntry]:
        return [BrokerHandlerEntry(*row) for row in rows]


_LISTEN_QUERY = SQL("LISTEN {}")

_UNLISTEN_QUERY = SQL("UNLISTEN {}")

# noinspection SqlDerivedTableAlias
_COUNT_NOT_PROCESSED_QUERY = SQL(
    "SELECT COUNT(*) FROM (SELECT id FROM consumer_queue WHERE topic = %s FOR UPDATE SKIP LOCKED) s"
)

_SELECT_NOT_PROCESSED_ROWS_QUERY = SQL(
    "SELECT id, topic, partition, data, retry, created_at, updated_at "
    "FROM consumer_queue "
    "WHERE topic = %s "
    "ORDER BY created_at "
    "LIMIT %s "
    "FOR UPDATE SKIP LOCKED"
)
_DELETE_PROCESSED_QUERY = SQL("DELETE FROM consumer_queue WHERE id = %s")
