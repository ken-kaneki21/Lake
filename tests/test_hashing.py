import unittest
from decimal import Decimal
from hashlib import md5

from recon.hashing import amount, bucket, fingerprint


class HashingTest(unittest.TestCase):
    def test_bucket_is_first_byte_of_key_digest(self):
        self.assertEqual(bucket(1), 196)
        self.assertEqual(bucket(100), 248)

    def test_exact_money_and_null(self):
        self.assertEqual(amount(Decimal("-0.00")), "0.00")
        self.assertEqual(amount(Decimal("-1.2")), "-1.20")
        self.assertEqual(amount(None), "~")
        with self.assertRaises(ValueError):
            amount(Decimal("0.001"))

    def test_delete_changes_fingerprint_and_retains_values(self):
        for deleted, expected in [(False, b"41|12.34|~|0"), (True, b"41|12.34|~|1")]:
            with self.subTest(deleted=deleted):
                digest = md5(expected, usedforsecurity=False).digest()
                self.assertEqual(
                    fingerprint(41, Decimal("12.34"), None, deleted),
                    int.from_bytes(digest[:4], "big"),
                )


if __name__ == "__main__":
    unittest.main()
