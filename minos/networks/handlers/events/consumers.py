# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

from typing import (
    Any,
)

from minos.common import (
    Event,
    MinosConfig,
)

from ..abc import (
    Consumer,
)


class EventConsumer(Consumer):
    """Event Consumer class."""

    TABLE_NAME = "event_queue"

    def __init__(self, *, config: MinosConfig, **kwargs: Any):
        super().__init__(config=config.events, **kwargs)
        self._kafka_conn_data = f"{config.events.broker.host}:{config.events.broker.port}"

    def _is_valid_instance(self, value: bytes):
        try:
            Event.from_avro_bytes(value)
            return True
        except:  # noqa E722
            return False
