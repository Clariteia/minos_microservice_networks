from __future__ import (
    annotations,
)

from contextvars import (
    ContextVar,
)
from enum import (
    Enum,
    IntEnum,
)
from typing import (
    Any,
    Final,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DeclarativeModel,
)

REPLY_TOPIC_CONTEXT_VAR: Final[ContextVar[Optional[str]]] = ContextVar("reply_topic", default=None)
TRACE_CONTEXT_VAR: Final[ContextVar[Optional[list[TraceStep]]]] = ContextVar("trace", default=None)


class TraceStep(DeclarativeModel):
    """TODO"""

    identifier: UUID
    service_name: str


class BrokerMessage(DeclarativeModel):
    """Broker Message class."""

    topic: str
    data: Any
    reply_topic: Optional[str]
    user: Optional[UUID]
    status: BrokerMessageStatus
    strategy: BrokerMessageStrategy

    trace: list[TraceStep]
    headers: dict[str, str]

    def __init__(
        self,
        topic: str,
        data: Any,
        trace: list[TraceStep],
        *,
        status: Optional[BrokerMessageStatus] = None,
        strategy: Optional[BrokerMessageStrategy] = None,
        headers: Optional[dict[str, str]] = None,
        **kwargs
    ):
        if status is None:
            status = BrokerMessageStatus.SUCCESS
        if strategy is None:
            strategy = BrokerMessageStrategy.UNICAST
        if headers is None:
            headers = dict()

        super().__init__(topic, data, trace=list(trace), status=status, strategy=strategy, headers=headers, **kwargs)

    @property
    def ok(self) -> bool:
        """Check if the reply is okay or not.

        :return: ``True`` if the reply is okay or ``False`` otherwise.
        """
        return self.status == BrokerMessageStatus.SUCCESS

    @property
    def identifier(self) -> UUID:
        """TODO"""
        return self.trace[-1].identifier

    @property
    def service_name(self) -> str:
        """TODO"""
        return self.trace[-1].service_name


class BrokerMessageStatus(IntEnum):
    """Broker Message Status class."""

    SUCCESS = 200
    ERROR = 400
    SYSTEM_ERROR = 500


class BrokerMessageStrategy(str, Enum):
    """Broker Message Strategy class"""

    UNICAST = "unicast"
    MULTICAST = "multicast"
