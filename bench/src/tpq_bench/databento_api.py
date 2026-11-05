"""Databento's historical API: a paid request, so a key, a price check, then the download.

The tests fake Databento's client (`tests/test_databento_api.py`): a real request needs a key
and spends credits.
"""

from __future__ import annotations

import dataclasses
import os
from typing import TYPE_CHECKING

from tpq_bench.fetch import Opener, Unavailable

if TYPE_CHECKING:
    import pathlib

KEY = "DATABENTO_API_KEY"
# no request costing more than this (USD) is made; `TPQ_BENCH_DATABENTO_MAX_COST` overrides it
MAX_COST = 2.0


@dataclasses.dataclass(frozen=True)
class Databento:
    dataset: str
    symbols: str
    stype_in: str
    schema: str
    start: str
    end: str

    def __call__(self, dest: pathlib.Path, opener: Opener | None = None) -> None:
        key = os.environ.get(KEY)
        if not key:
            raise Unavailable(f"set {KEY} to fetch from Databento's API, or pass DBN files with --dbn")
        try:
            import databento
        except ImportError as e:
            raise Unavailable("the Databento client is missing: install tpq-bench[databento]") from e
        client = databento.Historical(key)
        cost = client.metadata.get_cost(
            dataset=self.dataset,
            symbols=self.symbols,
            stype_in=self.stype_in,
            schema=self.schema,
            start=self.start,
            end=self.end,
        )
        limit = float(os.environ.get("TPQ_BENCH_DATABENTO_MAX_COST", MAX_COST))
        if cost > limit:
            raise Unavailable(
                f"{self.schema} would cost ${cost:.2f}, above the ${limit:.2f} limit "
                "(TPQ_BENCH_DATABENTO_MAX_COST raises it)"
            )
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        try:
            client.timeseries.get_range(
                dataset=self.dataset,
                symbols=self.symbols,
                stype_in=self.stype_in,
                schema=self.schema,
                start=self.start,
                end=self.end,
                path=part,
            )
            part.replace(dest)
        finally:
            part.unlink(missing_ok=True)
