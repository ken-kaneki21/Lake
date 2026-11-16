"""Remove raw ephemeral runner credentials from uploaded Compose diagnostics."""

import os
import sys

CREDENTIALS = ("PGPASSWORD", "CDC_PGPASSWORD", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")


def main():
    values = sorted({os.environ[name] for name in CREDENTIALS if os.environ.get(name)},
                    key=len, reverse=True)
    for line in sys.stdin:
        for value in values:
            line = line.replace(value, "[REDACTED]")
        sys.stdout.write(line)


if __name__ == "__main__":
    main()
