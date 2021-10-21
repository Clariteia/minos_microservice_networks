import unittest

from minos.common import (
    MinosException,
)
from minos.networks import (
    HandlerEntry,
)
from tests.utils import (
    FakeModel,
)


class TestHandlerEntry(unittest.TestCase):
    def test_data(self):
        entry = HandlerEntry(1, "fooReply", 0, FakeModel("test1").avro_bytes)
        self.assertEqual(FakeModel("test1"), entry.data)

    def test_data_not_deserializable(self):
        entry = HandlerEntry(1, "fooReply", 0, bytes())
        with self.assertRaises(MinosException):
            entry.data


if __name__ == "__main__":
    unittest.main()
