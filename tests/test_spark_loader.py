import unittest
from src.spark_loader import (
    _version_is_not_newer,
    _safe_int,
    _docs_equal_ignoring_id,
    _only_dup_key,
)


class TestSparkLoader(unittest.TestCase):
    def test_spark_safe_int(self):
        self.assertEqual(_safe_int("123"), 123)
        self.assertEqual(_safe_int(456), 456)
        self.assertIsNone(_safe_int(None))
        self.assertIsNone(_safe_int("abc"))
        self.assertIsNone(_safe_int(""))

    def test_spark_version_is_not_newer(self):
        # Newer incoming version -> should return False (i.e. incoming IS newer)
        existing = {"order_id": "ord-1", "version": 1}
        incoming_newer = {"order_id": "ord-1", "version": 2}
        self.assertFalse(_version_is_not_newer(existing, incoming_newer, "version"))

        # Same version -> should return True (incoming is NOT newer)
        incoming_same = {"order_id": "ord-1", "version": 1}
        self.assertTrue(_version_is_not_newer(existing, incoming_same, "version"))

        # Older version -> should return True (incoming is NOT newer)
        incoming_older = {"order_id": "ord-1", "version": 0}
        self.assertTrue(_version_is_not_newer(existing, incoming_older, "version"))

        # Unversioned incoming against versioned existing -> should return True (not newer)
        incoming_none = {"order_id": "ord-1"}
        self.assertTrue(_version_is_not_newer(existing, incoming_none, "version"))

    def test_spark_docs_equal_ignoring_id(self):
        doc1 = {"_id": "mongo-1", "order_id": "ord-1", "amount": 100}
        doc2 = {"_id": "mongo-2", "order_id": "ord-1", "amount": 100}
        doc3 = {"_id": "mongo-1", "order_id": "ord-1", "amount": 200}

        self.assertTrue(_docs_equal_ignoring_id(doc1, doc2))
        self.assertFalse(_docs_equal_ignoring_id(doc1, doc3))

    def test_spark_only_dup_key(self):
        class DummyError(Exception):
            def __init__(self, write_errors):
                self.details = {"writeErrors": write_errors}

        dup_error = DummyError([{"code": 11000}])
        self.assertTrue(_only_dup_key(dup_error))

        mixed_error = DummyError([{"code": 11000}, {"code": 12000}])
        self.assertFalse(_only_dup_key(mixed_error))

        other_error = Exception("generic")
        self.assertFalse(_only_dup_key(other_error))


if __name__ == "__main__":
    unittest.main()
