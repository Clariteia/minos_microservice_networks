import unittest
from abc import (
    ABC,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.networks import (
    Request,
    Response,
)
from tests.utils import (
    FakeModel,
)


class TestRequest(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(Request, ABC))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"__eq__", "__repr__", "has_content", "has_params", "user"}, Request.__abstractmethods__,
        )

    async def test_content_raises(self):
        class _Request(Request):
            @property
            def user(self) -> Optional[UUID]:
                """For testing purposes."""
                return None

            @property
            def has_content(self) -> bool:
                """For testing purposes."""
                return True

            @property
            def has_params(self) -> bool:
                """For testing purposes."""
                return True

            def __eq__(self, other: Request) -> bool:
                return True

            def __repr__(self) -> str:
                return str()

        with self.assertRaises(RuntimeError):
            await _Request().content()

    async def test_params_raises(self):
        class _Request(Request):
            @property
            def user(self) -> Optional[UUID]:
                """For testing purposes."""
                return None

            @property
            def has_content(self) -> bool:
                """For testing purposes."""
                return True

            @property
            def has_params(self) -> bool:
                """For testing purposes."""
                return True

            def __eq__(self, other: Request) -> bool:
                return True

            def __repr__(self) -> str:
                return str()

        with self.assertRaises(RuntimeError):
            await _Request().params()


class TestResponse(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.data = [FakeModel("blue"), FakeModel("red")]

    async def test_content(self):
        response = Response(self.data)
        self.assertEqual(self.data, await response.content())

    async def test_content_single(self):
        response = Response(self.data[0])
        self.assertEqual(self.data[0], await response.content())

    async def test_content_simple(self):
        response = Response(1234)
        self.assertEqual(1234, await response.content())

    async def test_raw_content(self):
        response = Response(self.data)
        self.assertEqual([item.avro_data for item in self.data], await response.raw_content())

    async def test_raw_content_single(self):
        response = Response(self.data[0])
        self.assertEqual(self.data[0].avro_data, await response.raw_content())

    async def test_raw_content_simple(self):
        response = Response(1234)
        self.assertEqual(1234, await response.raw_content())

    async def test_eq_true(self):
        self.assertEqual(Response(self.data), Response(self.data))

    async def test_eq_false(self):
        self.assertNotEqual(Response(self.data[0]), Response(self.data[1]))

    async def test_repr(self):
        response = Response(self.data)
        self.assertEqual(f"Response({self.data!r})", repr(response))

    def test_hash(self):
        self.assertIsInstance(hash(Response("test")), int)


if __name__ == "__main__":
    unittest.main()
