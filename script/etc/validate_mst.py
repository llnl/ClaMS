# Copyright 2023-2026 Lawrence Livermore National Security, LLC and other ClaMS
# Project Developers. See the top-level COPYRIGHT file for details.


#!/usr/bin/env python3
"""
mstvalidate_int.py — Validate an MST file (or a directory of files)

Checks:
  1) No duplicate edges (undirected: (u,v) == (v,u))
  2) Each edge line is valid: u, v are INT; w is FLOAT
  3) Prints edge weight sum at the end

Usage:
  # Single file
  python mstvalidate_int.py mst.txt

  # Directory (non-recursive): validate each file
  python mstvalidate_int.py path/to/dir

Input format (STRICT):
  Each non-empty, non-comment line must be:
      u  v  w
  where u, v are integers and w is a float.
  Delimiters: spaces/tabs OR commas.
  Lines starting with '#' are ignored.

Exit codes:
  0 — all inputs valid
  2 — one or more inputs invalid
  1 — usage or file errors
"""

from __future__ import annotations
import sys
from pathlib import Path
from typing import Dict, Tuple, List

Edge = Tuple[int, int]


def parse_line(line: str, lineno: int, path: Path):
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    parts = [p.strip() for p in (s.split(",") if "," in s else s.split())]
    if len(parts) < 3:
        raise ValueError(f"{path}:{lineno}: Expected 'u v w' (got: {line!r})")
    try:
        u, v = int(parts[0]), int(parts[1])
    except ValueError:
        raise ValueError(f"{path}:{lineno}: Node IDs must be integers (got: {parts[0]!r} {parts[1]!r})")
    try:
        w = float(parts[2])
    except ValueError:
        raise ValueError(f"{path}:{lineno}: Weight must be a float (got: {parts[2]!r})")
    # Normalize undirected edge
    if u <= v:
        return (u, v, w)
    else:
        return (v, u, w)


def validate_file(path: Path) -> Tuple[bool, int, float]:
    """
    Returns (is_valid, edge_count, weight_sum).
    Prints detailed errors to stderr when invalid.
    """
    seen: Dict[Edge, int] = {}  # edge -> first seen line number
    weight_sum: float = 0.0
    edges_count = 0

    with path.open("r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            parsed = parse_line(raw, lineno, path)
            if parsed is None:
                continue
            u, v, w = parsed
            key = (u, v)
            if key in seen:
                first = seen[key]
                print(f"ERROR: {path}:{lineno}: Duplicate edge ({u}, {v}); first seen at line {first}", file=sys.stderr)
                return (False, edges_count, weight_sum)
            seen[key] = lineno
            edges_count += 1
            weight_sum += w

    if edges_count == 0:
        print(f"ERROR: {path}: No edges found", file=sys.stderr)
        return (False, 0, 0.0)

    return (True, edges_count, weight_sum)


def fmt_weight(x: float) -> str:
    return f"{x:.12g}"


def main():
    if len(sys.argv) != 2:
        print("Usage: python mstvalidate_int.py <file-or-directory>", file=sys.stderr)
        sys.exit(1)

    target = Path(sys.argv[1])

    if target.is_file():
        ok, m, s = validate_file(target)
        if ok:
            print(f"Edges: {m}")
            print(f"Sum: {fmt_weight(s)}")  # edge weight sum at the end
            sys.exit(0)
        else:
            # (Errors already printed)
            sys.exit(2)

    elif target.is_dir():
        files = sorted(p for p in target.iterdir() if p.is_file())
        if not files:
            print(f"{target}: no regular files found", file=sys.stderr)
            sys.exit(1)

        any_fail = False
        grand_total = 0.0

        print(f"{'File':<40} {'#Edges':>8} {'Sum':>16} {'Status':>10}")
        print("-" * 80)
        for p in files:
            try:
                ok, m, s = validate_file(p)
                status = "OK" if ok else "INVALID"
                any_fail = any_fail or (not ok)
                if ok:
                    grand_total += s
                    print(f"{p.name:<40} {m:>8d} {fmt_weight(s):>16} {status:>10}")
                else:
                    print(f"{p.name:<40} {'-':>8} {'-':>16} {status:>10}")
            except Exception as e:
                any_fail = True
                print(f"{p.name:<40} {'-':>8} {'-':>16} {'ERROR':>10}", file=sys.stderr)
                print(f"ERROR: {e}", file=sys.stderr)

        print("-" * 80)
        print(f"{'TOTAL':<40} {'':>8} {fmt_weight(grand_total):>16} {'':>10}")  # final sum line

        sys.exit(0 if not any_fail else 2)

    else:
        print("Error: path is neither a file nor a directory.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
