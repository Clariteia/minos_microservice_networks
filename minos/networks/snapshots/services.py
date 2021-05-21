"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from aiomisc.service.periodic import (
    PeriodicService,
)

from minos.common import (
    MinosConfig,
)

from .dispatchers import (
    Snapshot,
)


class SnapshotService(PeriodicService):
    """Minos Snapshot Service class."""

    def __init__(self, config: MinosConfig = None, **kwargs):
        super().__init__(**kwargs)
        self.dispatcher = Snapshot.from_config(config=config)

    async def start(self) -> None:
        """Start the service execution.

        :return: This method does not return anything.
        """
        await self.dispatcher.setup()
        await super().start()

    async def callback(self) -> None:
        """Callback implementation to be executed periodically.

        :return: This method does not return anything.
        """
        await self.dispatcher.dispatch()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await super().stop(err)
        await self.dispatcher.destroy()
