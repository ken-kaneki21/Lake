"""Print the dbt parse exception when the console entry point exits silently."""

import glob
import sys
import traceback

sys.path[:0] = glob.glob("/opt/spark/python/lib/*.zip")

from dbt.cli.main import dbtRunner


def main():
    result = dbtRunner().invoke([
        "--debug", "--log-path", "/tmp/paritylake-dbt-logs", "parse", *sys.argv[1:],
    ])
    if result.exception is not None:
        traceback.print_exception(result.exception)
    else:
        print(f"Diagnostic dbtRunner parse success: {result.success}", file=sys.stderr)


if __name__ == "__main__":
    main()
