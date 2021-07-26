"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    ABC,
)
from typing import (
    Final,
    Iterable,
)

from .abc import (
    EnrouteDecorator,
)
from .kinds import (
    EnrouteDecoratorKind,
)


class BrokerEnrouteDecorator(EnrouteDecorator, ABC):
    """Broker Enroute class"""

    def __init__(self, topics: Iterable[str]):
        if isinstance(topics, str):
            topics = (topics,)
        self.topics = tuple(topics)

    def __iter__(self) -> Iterable:
        yield from (self.topics,)


class BrokerCommandEnrouteDecorator(BrokerEnrouteDecorator):
    """Broker Command Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Command


class BrokerQueryEnrouteDecorator(BrokerEnrouteDecorator):
    """Broker Query Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Query


class BrokerEventEnrouteDecorator(BrokerEnrouteDecorator):
    """Broker Event Enroute class"""

    KIND: Final[EnrouteDecoratorKind] = EnrouteDecoratorKind.Event
