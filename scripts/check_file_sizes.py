#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
from fnmatch import fnmatch
from pathlib import Path


def load_config() -> dict:
    return json.loads(Path(".size-budget.json").read_text())


def tracked_files() -> list[str]:
    output = subprocess.check_output(["git", "ls-files"], text=True)
    return [line.strip() for line in output.splitlines() if line.strip()]


def matches_any(path: str, patterns: list[str]) -> bool:
    return any(fnmatch(path, pattern) for pattern in patterns)


def file_metrics(path: Path) -> tuple[int, int]:
    return sum(1 for _ in path.open()), path.stat().st_size


def main() -> int:
    config = load_config()
    include = config["include"]
    exclude = config.get("exclude", [])
    default_limits = config["default"]
    exceptions = config.get("exceptions", {})

    violations: list[str] = []
    for rel in tracked_files():
        if not matches_any(rel, include) or matches_any(rel, exclude):
            continue
        path = Path(rel)
        if not path.exists():
            continue
        lines, bytes_ = file_metrics(path)
        limits = exceptions.get(rel, default_limits)
        max_lines = int(limits["max_lines"])
        max_bytes = int(limits["max_bytes"])
        if lines > max_lines or bytes_ > max_bytes:
            reason = limits.get("reason", "default budget")
            violations.append(
                f"{rel}: lines={lines}/{max_lines} bytes={bytes_}/{max_bytes} reason={reason}"
            )

    if violations:
        print("File size budget violations:")
        for violation in violations:
            print(f" - {violation}")
        return 1

    print("File size budget check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
