"""Seed corpus of pcodec_decompress_fuzz: each payload vector of TESTDATA/payloads as its
4-byte little-endian expected size followed by the payload.

    python3 cpp/fuzz/seeds.py TESTDATA OUT
"""

import json
import pathlib
import struct
import sys


def main(testdata: str, out: str) -> int:
    payloads = pathlib.Path(testdata) / "payloads"
    manifest = json.loads((payloads / "manifest.json").read_text())
    out_dir = pathlib.Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    for v in manifest["vectors"]:
        data = struct.pack("<I", v["expected_len"]) + (payloads / v["payload"]).read_bytes()
        (out_dir / v["name"]).write_bytes(data)
    print(f"{len(manifest['vectors'])} seeds in {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
