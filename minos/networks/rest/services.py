"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from aiomisc import bind_socket
from aiomisc.service.aiohttp import AIOHTTPService
from cached_property import cached_property

from minos.common import MinosConfig

from .builders import RestBuilder


class RestService(AIOHTTPService):
    """
    Rest Interface

    Expose REST Interface handler using aiomisc AIOHTTPService.

    """

    def __init__(self, **kwargs):
        super().__init__(**({"port": 9999} | kwargs))
        self._init_kwargs = kwargs

    async def create_application(self):
        """TODO

        :return: TODO
        """
        return self.builder.get_app()  # pragma: no cover

    @cached_property
    def builder(self) -> RestBuilder:
        """TODO

        :return: TODO
        """
        builder = RestBuilder.from_config(**self._init_kwargs)
        self.socket = bind_socket(address=builder.host, port=builder.port, proto_name="http",)
        return builder
