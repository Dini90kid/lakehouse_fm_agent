#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fmtool.py — Lakehouse FM Agent CLI (robust imports for Streamlit/Jobs)

Usage (module form is recommended):
  python -m lakehouse_fm_agent.fmtool --help

Also works as a script:
  python lakehouse_fm_agent/fmtool.py --help
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# ------------------------------------------------------------------------------
# Make sure the repo root is on sys.path so 'lakehouse_fm_agent' is importable
# This works even if Streamlit sets CWD to /mount/src (one level above package).
# ------------------------------------------------------------------------------
_THIS_FILE = Path(__file__).resolve()
_PKG_DIR   = _THIS_FILE.parent                    # .../lakehouse_fm_agent
_REPO_ROOT = _PKG_DIR.parent                      # .../

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Optional hard guards: verify init files exist (helps surface mis-naming quickly)
for must_exist in [
    _PKG_DIR / "__init__.py",
    _PKG_DIR / "runtime" / "__init__.py",
]:
    if not must_exist.exists():
        raise ModuleNotFoundError(
            f"Package bootstrap error: missing {must_exist}. "
            "Ensure 'lakehouse_fm_agent' and subpackages contain __init__.py files."
        )

# ------------------------------------------------------------------------------
# Absolute imports ONLY (no relative 'from runtime...' to avoid path issues)
# ------------------------------------------------------------------------------
from lakehouse_fm_agent.runtime.lineage import load_edges, topo_layers   # type: ignore
from lakehouse_fm_agent.runtime.registry import resolve_handler, POINTERS  # type: ignore
from lakehouse_fm_agent.runtime.manifest import Manifest                  # type: ignore


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _echo(msg: str) -> None:
    print(msg, flush=True)


def _fail(msg: str, exit_code: int = 2) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)
    sys.exit(exit_code)


# ------------------------------------------------------------------------------
# Commands
# ------------------------------------------------------------------------------
def cmd_graph(args: argparse.Namespace) -> None:
    """Build a Mermaid graph from LINEAGE.txt"""
    lineage_path = Path(args.lineage)
    out_path = Path(args.out)

    if not lineage_path.exists():
        _fail(f"Lineage file not found: {lineage_path}")

    edges = load_edges(str(lineage_path))
    lines = ["graph TD"]
    for parent, child, kind, _skipped in edges:
        lines.append(f'  "{parent}" --> "{child}"')

    out_path.write_text("\n".join(lines), encoding="utf-8")
    _echo(f"Mermaid saved: {out_path}")


def cmd_plan(args: argparse.Namespace) -> None:
    """Produce a PLANNED object manifest (JSON) for review/approval."""
    m = Manifest(plan_id=args.plan_id)
    # Minimal references; tailor to your UC
    m.ensure_table("uc_catalog", "ref", "fx_rates", "FX conversion reference", [])
    m.ensure_table("uc_catalog", "ref", "currency_decimals", "Currency decimals", [])
    m.ensure_table("uc_catalog", "ref", "uom_factors", "UoM base/factors", [])
    m.ensure_table("uc_catalog", "ref", "calendar", "Enterprise calendar", [])
    m.ensure_table("uc_catalog", "ref", "logsys_map", "Logical system map", [])

    out_path = Path(args.out)
    out_path.write_text(m.to_json(), encoding="utf-8")
    _echo(f"Manifest saved: {out_path}")


def cmd_scaffold(args: argparse.Namespace) -> None:
    """Generate doc stubs + handler skeleton for a given FM."""
    fm = args.fm.upper()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "master.md").write_text(
        f"# {fm} — Master Doc\n\nAS-IS, TO-BE, lineage, pointers, code.\n",
        encoding="utf-8",
    )
    (out_dir / "design_lld.md").write_text(
        f"# {fm} — Design LLD\n\nComponents, contracts, code slices.\n",
        encoding="utf-8",
    )
    (out_dir / "how_to_test.md").write_text(
        f"# {fm} — How to Test\n\nUnit + integration tests.\n",
        encoding="utf-8",
    )

    handler_src = (
        "from lakehouse_fm_agent.runtime.context import Context\n\n\n"
        "def handler(ctx: Context) -> None:\n"
        '    """\n'
        f"    Handler for {fm}. Implement per Master/LLD.\n"
        "    - Read inputs (ctx.current_df or ctx.read_uc(...))\n"
        "    - Apply Spark SQL / UDF / joins according to patterns\n"
        "    - Write outputs or assign ctx.current_df\n"
        '    """\n'
        "    pass\n"
    )
    (out_dir / "handler.py").write_text(handler_src, encoding="utf-8")
    _echo(f"Scaffold created under: {out_dir}")


def cmd_validate(args: argparse.Namespace) -> None:
    """Stub: extend with schema checks & config sanity."""
    if args.lineage and not Path(args.lineage).exists():
        _fail(f"Provided lineage path not found: {args.lineage}")
    if args.config and not Path(args.config).exists():
        _fail(f"Provided config path not found: {args.config}")
    _echo("Validate: OK (stub).")


def cmd_test(args: argparse.Namespace) -> None:
    """Stub: wire to pytest/nose as needed."""
    _echo(f"Run tests for FM={args.fm} (stub).")


def cmd_run(args: argparse.Namespace) -> None:
    """Execute handlers in topological order from LINEAGE.txt."""
    lineage_path = Path(args.lineage)
    if not lineage_path.exists():
        _fail(f"Lineage file not found: {lineage_path}")

    edges = load_edges(str(lineage_path))
    layers = topo_layers(edges)

    # Flatten and de-duplicate (preserve order)
    ordered = []
    seen = set()
    for layer in layers:
        for fm in layer:
            if fm not in seen:
                seen.add(fm)
                ordered.append(fm)

    _echo(f"Execution order (topological): {', '.join(ordered)}")

    for fm in ordered:
        fn = resolve_handler(fm)
        if fn:
            _echo(f"[RUN] {fm} -> handler")
            try:
                fn(None)  # In Databricks, pass a real Context with Spark
            except Exception as ex:
                _fail(f"Handler for {fm} raised an exception: {ex}")
        else:
            ptr = POINTERS.get(fm.upper())
            if ptr:
                _echo(f"[POINTER] {fm}: {ptr}")
            else:
                _echo(f"[SKIP] {fm}: no handler (BW pattern handled elsewhere)")


# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser("fmtool", description="Lakehouse FM Agent CLI")
    sp = ap.add_subparsers(dest="command")

    p = sp.add_parser("graph", help="Render Mermaid from LINEAGE.txt")
    p.add_argument("--lineage", required=True, help="Path to LINEAGE.txt")
    p.add_argument("--out", required=True, help="Path to output .mmd file")
    p.set_defaults(fn=cmd_graph)

    p = sp.add_parser("plan", help="Produce a PLANNED object manifest (JSON)")
    p.add_argument("--lineage", required=False, help="Path to LINEAGE.txt (optional)")
    p.add_argument("--plan-id", required=True, help="Plan id / batch reference")
    p.add_argument("--out", required=True, help="Path to output manifest.json")
    p.set_defaults(fn=cmd_plan)

    p = sp.add_parser("scaffold", help="Create docs + handler skeleton for an FM")
    p.add_argument("--fm", required=True, help="FM name (e.g., Y_DNP_CONV_BUOM_SU_SSU)")
    p.add_argument("--out", required=True, help="Output folder for scaffolding")
    p.set_defaults(fn=cmd_scaffold)

    p = sp.add_parser("validate", help="Validate config + lineage (stub)")
    p.add_argument("--config", required=False, help="Path to config (e.g., conf/tables.yml)")
    p.add_argument("--lineage", required=False, help="Path to LINEAGE.txt")
    p.set_defaults(fn=cmd_validate)

    p = sp.add_parser("test", help="Run unit/integration tests (stub)")
    p.add_argument("--fm", required=True, help="Target FM to test")
    p.set_defaults(fn=cmd_test)

    p = sp.add_parser("run", help="Execute handlers in topological order")
    p.add_argument("--lineage", required=True, help="Path to LINEAGE.txt")
    p.set_defaults(fn=cmd_run)

    return ap


def main(argv: list[str] | None = None) -> None:
    ap = build_parser()
    args = ap.parse_args(argv)
    if not hasattr(args, "fn"):
        ap.print_help()
        sys.exit(1)
    args.fn(args)


if __name__ == "__main__":
    main()
