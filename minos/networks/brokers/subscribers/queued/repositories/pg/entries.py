from __future__ import (
    annotations,
)

import logging
from datetime import (
    datetime,
)
from functools import (
    total_ordering,
)
from typing import (
    Any,
    Optional,
)

from cached_property import (
    cached_property,
)

from minos.common import (
    current_datetime,
)

from .....messages import (
    BrokerMessage,
)

logger = logging.getLogger(__name__)


@total_ordering
class PostgreSqlBrokerSubscriberRepositoryEntry:
    """Handler Entry class."""

    def __init__(
        self,
        id: int,
        topic: str,
        partition: int,
        data_bytes: bytes,
        retry: int = 0,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        if created_at is None or updated_at is None:
            now = current_datetime()
            if created_at is None:
                created_at = now
            if updated_at is None:
                updated_at = now

        self.id = id
        self.topic = topic
        self.partition = partition
        self.data_bytes = data_bytes
        self.retry = retry
        self.created_at = created_at
        self.updated_at = updated_at

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

    def __eq__(self, other):
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self):
        yield from (
            self.id,
            self.topic,
            self.partition,
            self.data_bytes,
            self.retry,
            self.created_at,
            self.updated_at,
        )

    def __repr__(self):
        return f"{type(self).__name__}({self.id!r}, {self.topic!r})"
