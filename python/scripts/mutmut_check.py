"""Runs mutmut and fails unless every mutant was killed (a timeout counts as killed: the
mutant made the tests hang). Run from python/ after `maturin develop`, with
TPARQUET_TESTDATA pointing at testdata/ (mutmut runs the tests from a copy)."""

from __future__ import annotations

import pathlib
import re
import shutil
import subprocess
import sys

CAUGHT = {"killed", "timeout"}


def refresh_extension() -> None:
    """mutmut copies the package into mutants/ once and keeps files that are already there:
    copy the freshly built extension (and the other non-Python files) again."""
    package = pathlib.Path("src/tparquet")
    target = pathlib.Path("mutants") / package
    if target.is_dir():
        for f in package.iterdir():
            if f.is_file() and f.suffix != ".py":
                shutil.copy2(f, target / f.name)


def main() -> int:
    refresh_extension()
    subprocess.run([sys.executable, "-m", "mutmut", "run", "--max-children", "2"], check=False)
    out = subprocess.run(
        [sys.executable, "-m", "mutmut", "results", "--all", "true"], capture_output=True, text=True, check=True
    ).stdout
    status: dict[str, str] = {}
    for line in out.splitlines():
        m = re.match(r"^\s*(\S+): (.+)$", line)
        if m:
            status[m.group(1)] = m.group(2).strip()
    counts: dict[str, int] = {}
    for s in status.values():
        counts[s] = counts.get(s, 0) + 1
    print("mutmut:", ", ".join(f"{n} {s}" for s, n in sorted(counts.items())))
    missed = sorted(name for name, s in status.items() if s not in CAUGHT)
    for name in missed:
        print(f"not killed: {name} ({status[name]})")
    if not status:
        print("mutmut reported no mutants")
        return 1
    return 1 if missed else 0


if __name__ == "__main__":
    raise SystemExit(main())
