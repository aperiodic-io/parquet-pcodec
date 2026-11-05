"""`tpq-bench list | fetch | run`: the providers' samples, as ZSTD(9) Parquet and as tparquet."""

from __future__ import annotations

import argparse
import os
import pathlib
import sys
import tempfile
from typing import TYPE_CHECKING

from tpq_bench import catalog, fetch, report
from tpq_bench.loaders import dbn_schema, load
from tpq_bench.measure import measure

if TYPE_CHECKING:
    from collections.abc import Sequence

DEFAULT_DATA = pathlib.Path(__file__).resolve().parents[2] / "data"


def _say(text: str) -> None:
    print(text, file=sys.stderr, flush=True)  # pragma: no mutate (progress shows as it happens)


def run(
    sources: Sequence[catalog.Source], data: pathlib.Path
) -> tuple[list[report.Entry], list[tuple[catalog.Source, str]]]:
    """Measures every source, fetching it first when it is not cached; returns the entries and
    the sources that could not be fetched, with the reason."""
    entries: list[report.Entry] = []
    skipped: list[tuple[catalog.Source, str]] = []
    for s in sources:
        try:
            path = fetch.ensure(s, data)
        except fetch.Unavailable as e:
            _say(f"skipped: {s.provider}: {s.data}: {e}")
            skipped.append((s, str(e)))
            continue
        _say(f"{s.provider}: {s.data} ...")
        with tempfile.TemporaryDirectory(dir=data, prefix="work-") as work:
            m = measure(load(s.loader, path), pathlib.Path(work))
        sizes = f"ZSTD(9) {report.size(m.zstd9_bytes)}, tparquet {report.size(m.tparquet_bytes)}"
        _say(f"  {m.rows:,} rows, {sizes}: {m.ratio:.3f}×")
        entries.append(report.Entry(s.provider, s.data, s.group, path.stat().st_size, m))
    return entries, skipped


def main(argv: Sequence[str] | None = None) -> int:
    # the program name and the help texts are for people only
    parser = argparse.ArgumentParser(prog="tpq-bench", description=__doc__)  # pragma: no mutate
    parser.add_argument("command", choices=["list", "fetch", "run"])
    parser.add_argument(
        "only",
        nargs="*",
        help="providers, data types or parts of cache keys (default: all)",  # pragma: no mutate
    )
    parser.add_argument(
        "--data",
        type=pathlib.Path,
        default=pathlib.Path(os.environ.get("TPQ_BENCH_DATA", DEFAULT_DATA)),
        help="the download cache (default: $TPQ_BENCH_DATA, else bench/data)",  # pragma: no mutate
    )
    parser.add_argument(
        "--dbn",
        type=pathlib.Path,
        nargs="+",
        default=[],
        help="DBN files from Databento's portal",  # pragma: no mutate
    )
    parser.add_argument(
        "--markdown",
        type=pathlib.Path,
        help="write the report here (default: standard output)",  # pragma: no mutate
    )
    parser.add_argument("--json", type=pathlib.Path, help="also write the results as JSON here")  # pragma: no mutate
    args = parser.parse_args(argv)

    sources = catalog.select(catalog.SOURCES, args.only)
    if args.command == "list":
        for s in sources:
            print(f"{s.group}  {s.key}  {'cached' if (args.data / s.key).exists() else 'missing'}")
        return 0
    args.data.mkdir(parents=True, exist_ok=True)
    if args.command == "fetch":
        failed = False
        for s in sources:
            try:
                fetch.ensure(s, args.data)
            except fetch.Unavailable as e:
                _say(f"not fetched: {s.provider}: {s.data}: {e}")
                failed = True
        return 1 if failed else 0

    sources += [catalog.dbn_file(p, dbn_schema(p)) for p in args.dbn]
    entries, _ = run(sources, args.data)
    if not entries:
        _say("nothing measured")
        return 1
    text = report.markdown(entries)
    if args.markdown is None:
        sys.stdout.write(text)
    else:
        args.markdown.write_text(text)
    if args.json is not None:
        args.json.write_text(report.to_json(entries))
    return 0


if __name__ == "__main__":
    sys.exit(main())
