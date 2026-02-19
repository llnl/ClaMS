# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


#!/usr/bin/env python3
"""
Convert marine dataset feature files to TSV with contiguous seq_id.
- One output TSV file per input file.
- One global ID mapping file.

Input format (whitespace-delimited):
  header_col0 header_col1 header_col2 ...
  ID0 fe0 fe1 fe2 ...
  ID1 fe0 fe1 fe2 ...
- First line is ignored (header).
- Original ID is a string (no whitespace).
- Features are floats.

Usage example:
python3 convert_marine_to_tsv_perfile.py "marine_raw/*.txt" \
  --out-dir marine_tsv \
  --map-out marine_index_to_seqid.tsv \
  --strict-dim
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
from typing import List, Optional, Tuple


def expand_inputs(patterns: List[str]) -> List[str]:
    """Expand globs; keep a stable, sorted order; dedupe."""
    seen = set()
    out: List[str] = []
    for p in patterns:
        matches = glob.glob(p)
        if not matches:
            matches = [p]
        for m in sorted(matches):
            ab = os.path.abspath(m)
            if ab in seen:
                continue
            if os.path.isfile(ab):
                seen.add(ab)
                out.append(ab)
    return out


def parse_row(line: str, path: str, line_no: int) -> Tuple[str, List[float]]:
    parts = line.strip().split()
    if not parts:
        raise ValueError(f"Blank row at {path}:{line_no}")
    orig_id = parts[0]
    if len(parts) < 2:
        raise ValueError(f"No features at {path}:{line_no} (orig_id={orig_id})")
    try:
        feats = [float(x) for x in parts[1:]]
    except ValueError as e:
        raise ValueError(f"Bad float at {path}:{line_no} (orig_id={orig_id}): {e}") from e
    return orig_id, feats


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Convert marine feature files to per-file TSV with seq_id + a global mapping TSV."
    )
    ap.add_argument(
        "inputs",
        nargs="+",
        help="Input file(s) or glob(s), e.g. data/*.txt",
    )
    ap.add_argument(
        "--out-dir",
        required=True,
        help="Output directory for per-input TSV files.",
    )
    ap.add_argument(
        "--map-out",
        required=True,
        help="Output TSV for global mapping: seq_id, file, orig_row_index, orig_id",
    )
    ap.add_argument(
        "--suffix",
        default=".tsv",
        help="Suffix for per-file outputs (default: .tsv).",
    )
    ap.add_argument(
        "--strict-dim",
        action="store_true",
        help="If set, enforce consistent feature dimension across ALL files/rows.",
    )
    ap.add_argument(
        "--skip-bad-rows",
        action="store_true",
        help="If set, skip rows that fail parsing instead of aborting.",
    )

    args = ap.parse_args()

    inputs = expand_inputs(args.inputs)
    if not inputs:
        print("ERROR: No input files found.", file=sys.stderr)
        return 2

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(args.map_out)) or ".", exist_ok=True)

    expected_dim: Optional[int] = None
    global_seq_id = 0
    total_written = 0

    with open(args.map_out, "w", encoding="utf-8") as fmap:
        fmap.write("seq_id\tfile\torig_row_index\torig_id\n")

        for in_path in inputs:
            base = os.path.basename(in_path)
            out_path = os.path.join(args.out_dir, base + args.suffix)

            wrote_this_file = 0
            with open(in_path, "r", encoding="utf-8") as fin, open(
                out_path, "w", encoding="utf-8"
            ) as fout:
                # Skip header
                header = fin.readline()
                if header == "":
                    print(f"WARNING: Empty file (no header): {in_path}", file=sys.stderr)
                    continue

                orig_row_index = 0  # 0-based among data rows (excluding header)
                for line_no, line in enumerate(fin, start=2):
                    if not line.strip():
                        continue

                    try:
                        orig_id, feats = parse_row(line, in_path, line_no)
                        dim = len(feats)
                        if expected_dim is None:
                            expected_dim = dim
                        elif args.strict_dim and dim != expected_dim:
                            raise ValueError(
                                f"Dim mismatch at {in_path}:{line_no} "
                                f"(orig_id={orig_id}): got {dim}, expected {expected_dim}"
                            )
                    except Exception as e:
                        if args.skip_bad_rows:
                            print(f"WARNING: {e} (skipped)", file=sys.stderr)
                            continue
                        print(f"ERROR: {e}", file=sys.stderr)
                        return 1

                    # Per-file TSV row: seq_id, features...
                    fout.write(str(global_seq_id))
                    for v in feats:
                        fout.write("\t")
                        fout.write(str(v))
                    fout.write("\n")

                    # Global mapping row
                    fmap.write(f"{global_seq_id}\t{base}\t{orig_row_index}\t{orig_id}\n")

                    global_seq_id += 1
                    orig_row_index += 1
                    wrote_this_file += 1
                    total_written += 1

            if wrote_this_file == 0:
                print(f"WARNING: Wrote 0 rows for {in_path}", file=sys.stderr)

    print(f"Done. Wrote {total_written} rows across {len(inputs)} file(s).", file=sys.stderr)
    print(f"Per-file TSVs in: {args.out_dir}", file=sys.stderr)
    print(f"Global mapping TSV: {args.map_out}", file=sys.stderr)
    if expected_dim is not None:
        print(f"Detected feature dimension: {expected_dim}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
