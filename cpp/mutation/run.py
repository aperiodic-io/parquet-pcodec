"""Mutation check of the Arrow fork's changed functions (cpp/arrow/FORK.md).

No C++ mutation tool (Mull, Dextool) is installable on the build machines, so this applies
the hand-picked mutations of mutants.json to target/cpp/arrow-src one at a time, rebuilds
and reinstalls the release Arrow incrementally (ccache makes the restores cheap), runs the
C++ test suite (target/cpp/build-release/tparquet-tests, which links the shared Arrow
libraries) and requires it to fail. A mutant that does not build counts as unviable, a hang
(the timeout) as caught. The source file is restored after every mutant, and Arrow is
rebuilt from the restored tree at the end.

    python3 cpp/mutation/run.py --root . [--jobs 3] [--only ID ...]

Writes target/cpp/mutation.out/results.json and exits 1 if any mutant survived.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import time


def run(cmd: list[str], timeout: float | None = None, log: pathlib.Path | None = None) -> int:
    out = open(log, "ab") if log else subprocess.DEVNULL
    try:
        return subprocess.run(cmd, stdout=out, stderr=subprocess.STDOUT, timeout=timeout).returncode
    except subprocess.TimeoutExpired:
        return -1
    finally:
        if log:
            out.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--jobs", default="3")
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--test-timeout", type=float, default=600)
    args = ap.parse_args()
    root = pathlib.Path(args.root).resolve()
    here = pathlib.Path(__file__).resolve().parent
    src = root / "target/cpp/arrow-src"
    build = root / "target/cpp/arrow-release/build"
    tests = root / "target/cpp/build-release/tparquet-tests"
    out = root / "target/cpp/mutation.out"
    out.mkdir(parents=True, exist_ok=True)
    log = out / "log.txt"
    log.write_text("")
    for p in (src / ".tparquet-fork", build / "build.ninja", tests):
        if not p.exists():
            print(f"mutants: {p} is missing: run `make -C cpp build` first", file=sys.stderr)
            return 2

    def rebuild() -> int:
        rc = run(["cmake", "--build", str(build), "-j", args.jobs], log=log)
        return rc if rc else run(["cmake", "--install", str(build)], log=log)

    mutants = json.loads((here / "mutants.json").read_text())["mutants"]
    if args.only:
        mutants = [m for m in mutants if m["id"] in args.only]
    # the unmutated tree must build and pass, or every mutant would look caught
    if rebuild() != 0 or run([str(tests)], timeout=args.test_timeout, log=log) != 0:
        print("mutants: the unmutated fork does not build or pass its tests", file=sys.stderr)
        return 2

    results = []
    try:
        for m in mutants:
            path = src / m["file"]
            original = path.read_text()
            if original.count(m["old"]) != 1:
                print(f"mutants: {m['id']}: `old` occurs {original.count(m['old'])} times in {m['file']}", file=sys.stderr)
                return 2
            start = time.time()
            with open(log, "a") as f:
                f.write(f"\n===== {m['id']}\n")
            try:
                path.write_text(original.replace(m["old"], m["new"]))
                if rebuild() != 0:
                    outcome = "unviable"
                else:
                    rc = run([str(tests), "--gtest_brief=1"], timeout=args.test_timeout, log=log)
                    outcome = "caught" if rc != 0 else "missed"
                    if rc == -1:
                        outcome = "caught (timeout)"
            finally:
                path.write_text(original)
            results.append({"id": m["id"], "file": m["file"], "outcome": outcome, "seconds": round(time.time() - start)})
            print(f"{outcome:16} {m['id']:40} {m['file']}", flush=True)
    finally:
        # the restored tree, built and installed again
        if rebuild() != 0:
            print("mutants: rebuilding the restored fork failed", file=sys.stderr)
    (out / "results.json").write_text(json.dumps(results, indent=1) + "\n")
    counts: dict[str, int] = {}
    for r in results:
        counts[r["outcome"].split()[0]] = counts.get(r["outcome"].split()[0], 0) + 1
    print(f"mutants: {len(results)} mutants: " + ", ".join(f"{v} {k}" for k, v in sorted(counts.items())))
    return 1 if counts.get("missed") else 0


if __name__ == "__main__":
    sys.exit(main())
