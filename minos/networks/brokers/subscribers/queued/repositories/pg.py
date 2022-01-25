from __future__ import (
    annotations,
)

import logging
from asyncio import (
    CancelledError,
    PriorityQueue,
    TimeoutError,
    create_task,
    wait_for,
)
from contextlib import (
    suppress,
)
from typing import (
    Any,
    NoReturn,
    Optional,
)

from aiopg import (
    Cursor,
)
from cached_property import (
    cached_property,
)
from psycopg2.sql import (
    SQL,
    Identifier,
)

from minos.common import (
    MinosConfig,
)

from .....utils import (
    consume_queue,
)
from ....collections import (
    PostgreSqlBrokerRepository,
)
from ....messages import (
    BrokerMessage,
)
from .abc import (
    BrokerSubscriberRepository,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerSubscriberRepository(PostgreSqlBrokerRepository, BrokerSubscriberRepository):
    """PostgreSql Broker Subscriber Repository class."""

    _queue: PriorityQueue[_Entry]

    _CREATE_TABLE_QUERY = SQL(
        "CREATE TABLE IF NOT EXISTS broker_subscriber_queue ("
        '"id" BIGSERIAL NOT NULL PRIMARY KEY, '
        '"topic" VARCHAR(255) NOT NULL, '
        '"data" BYTEA NOT NULL, '
        '"retry" INTEGER NOT NULL DEFAULT 0,'
        '"processing" BOOL NOT NULL DEFAULT FALSE, '
        '"created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(), '
        '"updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW())'
    )

    _INSERT_QUERY = SQL("INSERT INTO broker_subscriber_queue (topic, data) VALUES (%s, %s) RETURNING id")

    _NOTIFY_QUERY = SQL("NOTIFY {}")

    # noinspection SqlDerivedTableAlias
    _COUNT_NOT_PROCESSED_QUERY = SQL(
        "SELECT COUNT(*) "
        "FROM (SELECT id FROM broker_subscriber_queue WHERE "
        "NOT processing AND retry < %s AND topic IN %s FOR UPDATE SKIP LOCKED) s"
    )

    _SELECT_NOT_PROCESSED_QUERY = SQL(
        "SELECT id, data "
        "FROM broker_subscriber_queue "
        "WHERE NOT processing AND retry < %s AND topic IN %s "
        "ORDER BY created_at "
        "LIMIT %s "
        "FOR UPDATE SKIP LOCKED"
    )

    _MARK_PROCESSING_QUERY = SQL("UPDATE broker_subscriber_queue SET processing = TRUE WHERE id IN %s")

    _DELETE_PROCESSED_QUERY = SQL("DELETE FROM broker_subscriber_queue WHERE id = %s")

    _UPDATE_NOT_PROCESSED_QUERY = SQL(
        "UPDATE broker_subscriber_queue SET processing = FALSE, retry = retry + 1, updated_at = NOW() WHERE id = %s"
    )

    _LISTEN_QUERY = SQL("LISTEN {}")

    _UNLISTEN_QUERY = SQL("UNLISTEN {}")

    def __init__(self, topics: set[str], records: int, retry: int, **kwargs):
        super().__init__(topics, **kwargs)

        self._records = records
        self._retry = retry

        self._queue = PriorityQueue(maxsize=records)

        self._run_task = None

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlBrokerSubscriberRepository:
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_table()
        await self._start_run()

    async def _destroy(self) -> None:
        await self._stop_run()
        await self._flush_queue()
        await super()._destroy()

    async def _create_table(self) -> None:
        await self.submit_query(self._CREATE_TABLE_QUERY, lock=hash("broker_subscriber_queue"))

    async def _start_run(self):
        if self._run_task is None:
            self._run_task = create_task(self._run())

    async def _stop_run(self):
        if self._run_task is not None:
            self._run_task.cancel()
            with suppress(TimeoutError, CancelledError):
                await wait_for(self._run_task, 0.5)
            self._run_task = None

    async def _flush_queue(self):
        while not self._queue.empty():
            entry = self._queue.get_nowait()
            await self.submit_query(self._UPDATE_NOT_PROCESSED_QUERY, (entry.id_,))
            self._queue.task_done()

    async def _enqueue(self, message: BrokerMessage) -> None:
        params = (message.topic, message.avro_bytes)
        await self.submit_query_and_fetchone(self._INSERT_QUERY, params)
        await self.submit_query(self._NOTIFY_QUERY.format(Identifier(message.topic)))

    async def _dequeue(self) -> BrokerMessage:
        while True:
            entry = await self._queue.get()
            try:
                # noinspection PyBroadException
                try:
                    message = entry.data
                except Exception as exc:
                    logger.warning(
                        f"There was a problem while trying to deserialize the entry with {entry.id_!r} id: {exc}"
                    )
                    await self.submit_query(self._UPDATE_NOT_PROCESSED_QUERY, (entry.id_,))
                    continue

                await self.submit_query(self._DELETE_PROCESSED_QUERY, (entry.id_,))
                return message
            finally:
                self._queue.task_done()

    async def _run(self, max_wait: Optional[float] = 60.0) -> NoReturn:
        async with self.cursor() as cursor:
            await self._listen_entries(cursor)
            try:
                while True:
                    await self._wait_for_entries(cursor, max_wait)
                    await self._dequeue_batch(cursor)
            finally:
                await self._unlisten_entries(cursor)

    async def _listen_entries(self, cursor: Cursor):
        for topic in self.topics:
            # noinspection PyTypeChecker
            await cursor.execute(self._LISTEN_QUERY.format(Identifier(topic)))

    async def _unlisten_entries(self, cursor: Cursor) -> None:
        for topic in self.topics:
            # noinspection PyTypeChecker
            await cursor.execute(self._UNLISTEN_QUERY.format(Identifier(topic)))

    async def _wait_for_entries(self, cursor: Cursor, max_wait: Optional[float]) -> None:
        while True:
            if await self._get_count(cursor):
                return

            with suppress(TimeoutError):
                return await wait_for(consume_queue(cursor.connection.notifies, self._records), max_wait)

    async def _get_count(self, cursor) -> int:
        await cursor.execute(self._COUNT_NOT_PROCESSED_QUERY, (self._retry, tuple(self.topics)))
        count = (await cursor.fetchone())[0]
        return count

    async def _dequeue_batch(self, cursor: Cursor) -> None:
        async with cursor.begin():
            # noinspection PyTypeChecker
            await cursor.execute(self._SELECT_NOT_PROCESSED_QUERY, (self._retry, tuple(self.topics), self._records))
            rows = await cursor.fetchall()

            if not len(rows):
                return

            entries = [_Entry(*row) for row in rows]

            # noinspection PyTypeChecker
            await cursor.execute(self._MARK_PROCESSING_QUERY, (tuple(e.id_ for e in entries),))

            for entry in entries:
                await self._queue.put(entry)


class _Entry:
    """Handler Entry class."""

    def __init__(self, id_: int, data_bytes: bytes):

        self.id_ = id_
        self.data_bytes = data_bytes

    @cached_property
    def data(self) -> BrokerMessage:
        """Get the data.

        :return: A ``Model`` inherited instance.
        """
        return BrokerMessage.from_avro_bytes(self.data_bytes)

    def __lt__(self, other: Any) -> bool:
        # noinspection PyBroadException
        try:
            return isinstance(other, type(self)) and self.data < other.data
        except Exception:
            return False
