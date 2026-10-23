"""Backward-compatible entry point for the Checkpoint 2 environment smoke test."""
import sys
from .env_check import main

if __name__ == "__main__":
    # Preserve the earlier README command; env_check always runs the full smoke test.
    if "--check-spark" in sys.argv:
        sys.argv.remove("--check-spark")
    raise SystemExit(main())
