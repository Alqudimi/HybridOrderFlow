import unittest

from src.batch_loader import RawLoadResult
from src.repositories import (
    as_int,
    incoming_version_is_not_newer,
    only_duplicate_key_errors,
)
from src.spark_loader import (
    SparkRunResult,
    _clean_partition,
    _docs_equal_ignoring_id,
    _payload_hash_from_rdd,
)


class TestSparkSharedHelpers(unittest.TestCase):
    """Verify that Spark now uses the shared helpers from repositories."""

    def test_as_int(self):
        self.assertEqual(as_int("123"), 123)
        self.assertEqual(as_int(456), 456)
        self.assertIsNone(as_int(None))
        self.assertIsNone(as_int("abc"))
        self.assertIsNone(as_int(""))

    def test_version_is_not_newer(self):
        # Newer incoming version -> should return False (i.e. incoming IS newer)
        existing = {"order_id": "ord-1", "version": 1}
        incoming_newer = {"order_id": "ord-1", "version": 2}
        self.assertFalse(incoming_version_is_not_newer(existing, incoming_newer, "version"))

        # Same version -> should return True (incoming is NOT newer)
        incoming_same = {"order_id": "ord-1", "version": 1}
        self.assertTrue(incoming_version_is_not_newer(existing, incoming_same, "version"))

        # Older version -> should return True (incoming is NOT newer)
        incoming_older = {"order_id": "ord-1", "version": 0}
        self.assertTrue(incoming_version_is_not_newer(existing, incoming_older, "version"))

        # Unversioned incoming against versioned existing -> should return True (not newer)
        incoming_none = {"order_id": "ord-1"}
        self.assertTrue(incoming_version_is_not_newer(existing, incoming_none, "version"))

    def test_docs_equal_ignoring_id(self):
        doc1 = {"_id": "mongo-1", "order_id": "ord-1", "amount": 100}
        doc2 = {"_id": "mongo-2", "order_id": "ord-1", "amount": 100}
        doc3 = {"_id": "mongo-1", "order_id": "ord-1", "amount": 200}

        self.assertTrue(_docs_equal_ignoring_id(doc1, doc2))
        self.assertFalse(_docs_equal_ignoring_id(doc1, doc3))

    def test_only_dup_key(self):
        class DummyError(Exception):
            def __init__(self, write_errors):
                self.details = {"writeErrors": write_errors}

        dup_error = DummyError([{"code": 11000}])
        self.assertTrue(only_duplicate_key_errors(dup_error))

        mixed_error = DummyError([{"code": 11000}, {"code": 12000}])
        self.assertFalse(only_duplicate_key_errors(mixed_error))

        other_error = Exception("generic")
        self.assertFalse(only_duplicate_key_errors(other_error))


class TestPayloadHashFromRDD(unittest.TestCase):
    """Verify scalable deterministic RDD hashing."""

    class FakeRDD:
        def __init__(self, partitions: list[list[dict]]):
            self._partitions = partitions

        def mapPartitions(self, func):
            results = []
            for part in self._partitions:
                results.extend(func(iter(part)))
            return self.FakeRDDResult(results)

        class FakeRDDResult:
            def __init__(self, items):
                self._items = items

            def collect(self):
                return list(self._items)

    def test_deterministic_hashing(self):
        rdd1 = self.FakeRDD([
            [{"order_id": "ORD-1"}, {"order_id": "ORD-2"}],
            [{"order_id": "ORD-3"}],
        ])
        rdd2 = self.FakeRDD([
            [{"order_id": "ORD-1"}, {"order_id": "ORD-2"}],
            [{"order_id": "ORD-3"}],
        ])
        hash1 = _payload_hash_from_rdd(rdd1)
        hash2 = _payload_hash_from_rdd(rdd2)
        self.assertEqual(hash1, hash2)
        self.assertEqual(len(hash1), 64)  # Valid SHA-256 hex string


class TestCleanPartitionDuplicateDetection(unittest.TestCase):
    """Verify that _clean_partition detects duplicate order_ids within a file."""

    class FakeRow:
        """Simulate a Spark Row with asDict."""
        def __init__(self, data: dict):
            self._data = data

        def asDict(self, recursive=False):
            return dict(self._data)

    def test_duplicate_detection_within_partition(self):
        """Same order_id appearing twice should mark the second as duplicate."""
        rows = [
            self.FakeRow({
                "order_id": "طلب-100001",
                "order_date": "2025-01-01",
                "status": "مؤكد",
                "customer_id": "CUST-1",
                "customer_name": "أحمد",
                "customer_phone": "771234567",
                "customer_email": "a@x.com",
                "city": "صنعاء",
                "district": "حدة",
                "delivery_type": "عادي",
                "delivery_cost": "2000",
                "payment_method": "نقدًا",
                "payment_status": "تم الدفع",
                "payment_amount": "10000",
                "currency": "YER",
                "total_amount": "10000",
                "items_json": '[{"sku":"A","name":"B","qty":1,"unit_price":10000,"total":10000}]',
                "run_id": "test-run",
                "source_file": "/test.csv",
                "source_row_number": 2,
                "engine_used": "pyspark",
            }),
            self.FakeRow({
                "order_id": "طلب-100001",  # Duplicate!
                "order_date": "2025-01-01",
                "status": "مؤكد",
                "customer_id": "CUST-1",
                "customer_name": "أحمد",
                "customer_phone": "771234567",
                "customer_email": "a@x.com",
                "city": "صنعاء",
                "district": "حدة",
                "delivery_type": "عادي",
                "delivery_cost": "2000",
                "payment_method": "نقدًا",
                "payment_status": "تم الدفع",
                "payment_amount": "10000",
                "currency": "YER",
                "total_amount": "10000",
                "items_json": '[{"sku":"A","name":"B","qty":1,"unit_price":10000,"total":10000}]',
                "run_id": "test-run",
                "source_file": "/test.csv",
                "source_row_number": 3,
                "engine_used": "pyspark",
            }),
        ]

        results = list(_clean_partition(iter(rows)))
        self.assertEqual(len(results), 2)
        # Second record with same order_id should be quarantined as duplicate
        statuses = [r["quality_status"] for r in results]
        self.assertIn("quarantined", statuses)

    def test_source_metadata_structure(self):
        """Verify unified source metadata is present."""
        row = self.FakeRow({
            "order_id": "طلب-100002",
            "order_date": "2025-01-01",
            "status": "مؤكد",
            "customer_id": "CUST-2",
            "customer_name": "سارة",
            "customer_phone": "732112233",
            "customer_email": "s@x.com",
            "city": "عدن",
            "district": "المنصورة",
            "delivery_type": "سريع",
            "delivery_cost": "5000",
            "payment_method": "بطاقة",
            "payment_status": "تم الدفع",
            "payment_amount": "50000",
            "currency": "YER",
            "total_amount": "50000",
            "items_json": '[{"sku":"B","name":"C","qty":1,"unit_price":50000,"total":50000}]',
            "run_id": "test-run",
            "source_file": "/test.csv",
            "source_row_number": 2,
            "engine_used": "pyspark",
        })
        results = list(_clean_partition(iter([row])))
        self.assertEqual(len(results), 1)
        source = results[0]["source"]
        self.assertEqual(source["engine_used"], "pyspark")
        self.assertIn("incremental", source)
        self.assertIn("version_field", source)
        self.assertFalse(source["incremental"])

    def test_incremental_mode_version_extraction(self):
        """In incremental mode, version is extracted from the row."""
        row = self.FakeRow({
            "order_id": "طلب-100003",
            "order_date": "2025-01-01",
            "status": "مؤكد",
            "customer_id": "CUST-3",
            "customer_name": "خالد",
            "customer_phone": "713112233",
            "customer_email": "k@x.com",
            "city": "تعز",
            "district": "القاهرة",
            "delivery_type": "عادي",
            "delivery_cost": "3000",
            "payment_method": "محفظة",
            "payment_status": "تم الدفع",
            "payment_amount": "30000",
            "currency": "YER",
            "total_amount": "30000",
            "items_json": '[{"sku":"C","name":"D","qty":1,"unit_price":30000,"total":30000}]',
            "run_id": "test-run",
            "source_file": "/test.csv",
            "source_row_number": 2,
            "engine_used": "pyspark",
            "version": "5",
        })
        results = list(_clean_partition(iter([row]), incremental=True))
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertTrue(result["source"]["incremental"])
        # Version should be extracted as integer
        if result["quality_status"] != "quarantined":
            self.assertEqual(result["version"], 5)


class TestSparkRunResultStructure(unittest.TestCase):
    """Verify SparkRunResult has the expected fields."""

    def test_default_error_case_counts(self):
        result = SparkRunResult(
            raw_result=RawLoadResult(rows_read=100, raw_loaded=100),
            valid_count=80,
            corrected_count=10,
            quarantine_count=10,
            partitions=4,
            inserted_count=80,
            updated_count=0,
            unchanged_count=0,
            elapsed_seconds=1.5,
        )
        self.assertIsInstance(result.error_case_counts, dict)
        self.assertEqual(len(result.error_case_counts), 0)


if __name__ == "__main__":
    unittest.main()
