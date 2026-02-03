"""
Helper script to run tests and durability/throughput benchmarks
for the TCP-based key-value store implemented in `kv_store.py`.
"""

from kv_store import run_tests, run_benchmarks


def main():
    # Run correctness tests (Set/Get/Delete/BulkSet + persistence)
    run_tests()

    # Run throughput and durability benchmarks
    run_benchmarks()


if __name__ == "__main__":
    main()