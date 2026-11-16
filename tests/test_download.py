import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

from source_db.download import download, validate_zones

ZONES = b"LocationID,Borough,Zone,service_zone\n1,Test,Test,Test\n"


class Response(io.BytesIO):
    @property
    def headers(self):
        return {"Content-Length": str(len(self.getvalue()))}


class DownloadTest(unittest.TestCase):
    def test_retry_validates_and_renames(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "zones.csv"
            with (
                patch("source_db.download.urlopen", side_effect=[URLError("offline"), Response(ZONES)]) as fetch,
                patch("source_db.download.time.sleep") as wait,
            ):
                self.assertEqual(download("https://example.invalid/zones.csv", path, validate_zones), 1)
                self.assertEqual(fetch.call_count, 2)
                wait.assert_called_once_with(1)
            self.assertEqual(path.read_bytes(), ZONES)
            self.assertEqual(list(path.parent.iterdir()), [path])

    def test_invalid_download_does_not_replace_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "zones.csv"
            with (
                patch("source_db.download.urlopen", side_effect=lambda *a, **kw: Response(b"broken")),
                patch("source_db.download.time.sleep"),
            ):
                with self.assertRaisesRegex(RuntimeError, "Invalid TLC zone lookup"):
                    download("https://example.invalid/zones.csv", path, validate_zones)
            self.assertEqual(list(path.parent.iterdir()), [])

    def test_existing_file_is_validated_without_network(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "zones.csv"
            path.write_bytes(ZONES)
            with patch("source_db.download.urlopen") as fetch:
                self.assertEqual(download("https://example.invalid/zones.csv", path, validate_zones), 1)
            fetch.assert_not_called()
