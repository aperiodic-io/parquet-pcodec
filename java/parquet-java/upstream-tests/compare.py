"""Compares upstream's test outcomes against upstream parquet-java and against the fork.

    compare.py STOCK_REPORTS FORK_REPORTS

Each argument is a surefire reports directory (target/surefire-reports-<version>). Prints the
counts and exits 1 unless every test has the same outcome in both, and the fork ran tests.
"""

import pathlib
import sys
import xml.etree.ElementTree as ET


def outcomes(d: pathlib.Path) -> dict[str, str]:
    out = {}
    for f in sorted(d.glob("TEST-*.xml")):
        for tc in ET.parse(f).getroot().iter("testcase"):
            name = f"{tc.get('classname')}.{tc.get('name')}"
            kind = "passed"
            for k in ("failure", "error", "skipped"):
                if tc.find(k) is not None:
                    kind = k
            out[name] = kind
    return out


def main() -> int:
    stock, fork = (outcomes(pathlib.Path(a)) for a in sys.argv[1:3])
    for label, o in (("upstream", stock), ("fork", fork)):
        counts = {k: sum(1 for v in o.values() if v == k) for k in ("passed", "failure", "error", "skipped")}
        print(f"{label:9} {len(o)} tests: " + ", ".join(f"{v} {k}" for k, v in counts.items()))
    diff = sorted(n for n in stock.keys() | fork.keys() if stock.get(n) != fork.get(n))
    for n in diff:
        print(f"  {n}: upstream {stock.get(n, 'absent')}, fork {fork.get(n, 'absent')}")
    if not fork or diff:
        print("upstream tests: the fork's outcomes differ" if diff else "upstream tests: no tests ran")
        return 1
    print("upstream tests: same outcome for every test")
    return 0


if __name__ == "__main__":
    sys.exit(main())
