"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    Any,
    Union,
)

from minos.common import (
    Command,
    Event,
)

from ..messages import (
    Request,
    Response,
    ResponseException,
)


class HandlerRequest(Request):
    """Handler Request class."""

    __slots__ = "raw"

    def __init__(self, raw: Union[Command, Event]):
        self.raw = raw

    def __eq__(self, other: HandlerRequest) -> bool:
        return type(self) == type(other) and self.raw == other.raw

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw!r})"

    async def content(self, **kwargs) -> Any:
        """Request content.

        :param kwargs: Additional named arguments.
        :return: The command content.
        """
        data = self.raw.data
        return data


class HandlerResponse(Response):
    """Command Response class."""


class HandlerResponseException(ResponseException):
    """Command Response Exception class."""