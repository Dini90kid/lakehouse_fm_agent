#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sys
from pathlib import Path

from runtime.lineage import load_edges, topo_layers
from runtime.registry import resolve_handler, POINTERS
from runtime.manifest import Manifest


def cmd_graph(args):
    edges = load_edges(args.lineage)
    lines = ["graph TD"]
    for p, c, k, _s in edges:
        lines.append(f'  "{p}" --> "{c}"')
    Path(args.out).write_text("\n".join(lines), encoding="utf-8")
    print(f"Mermaid saved: {args.out}")


def cmd_plan(args):
    Manifest.ensure_defaults = True  # hint flag for later
    m = Manifest(plan_id=args.plan_id)
    # Minimal refs; customize to your UC before apply/create steps
    m.ensure_table("uc_catalog", "ref", "fx_rates", "FX conversion reference", [])
    m.ensure_table("uc_catalog", "ref", "currency_decimals", "Currency decimals", [])
    m.ensure_table("uc_catalog", "ref", "uom_factors", "UoM base/factors", [])
    m.ensure_table("uc_catalog", "ref", "calendar", "Enterprise calendar", [])
    m.ensure_table("uc_catalog", "ref", "logsys_map", "Logical system map", [])
    Path(args.out).write_text(m.to_json(), encoding="utf-8")
    print(f"Manifest saved: {args.out}")


def cmd_scaffold(args):
    fm = args.fm.upper()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "master.md").write_text(f"# {fm} — Master Doc\n\nAS-IS, TO-BE, lineage, pointers, code.", encoding="utf-8")
    (out / "design_lld.md").write_text(f"# {fm} — Design LLD\n\nComponents, contracts, code slices.", encoding="utf-8")
    (out / "how_to_test.md").write_text(f"# {fm} — How to Test\n\nUnit + integration tests.", encoding="utf-8")
    (out / "handler.py").write_text(
        (
            "from lakehouse_fm_agent.runtime.context import Context\n\n\n"
            "def handler(ctx: Context) -> None:\n"
            '    """\n'
            f"    Handler for {fm}. Implement per Master/LLD.\n"
            '    """\n'
            "    pass\n"
        ),
        encoding="utf-8",
    )
    print(f"Scaffold created under: {out}")


def cmd_validate(args):  # stub
    print("Validate config/lineage -- stub OK")


def cmd_test(args):  # stub
    print(f"Run unit/integration tests for {args.fm} -- stub OK")


def cmd_run(args):
    edges = load_edges(args.lineage)
    for layer in topo_layers(edges):
        for fm in layer:
            fn = resolve_handler(fm)
            if fn:
                print(f"[RUN] {fm} -> handler")
                fn(None)  # In Databricks, pass Context with Spark session
            else:
                ptr = POINTERS.get(fm.upper())
                if ptr:
                    print(f"[POINTER] {fm}: {ptr}")
                else:
                    print(f"[SKIP] {fm}: no handler; BW pattern replaced elsewhere")


def main():
    ap = argparse.ArgumentParser("fmtool")
    sp = ap.add_subparsers()

    p = sp.add_parser("graph")
    p.add_argument("--lineage", required=True)
    p.add_argument("--out", required=True)
    p.set_defaults(fn=cmd_graph)

    p = sp.add_parser("plan")
    p.add_argument("--lineage", required=True)
    p.add_argument("--plan-id", required=True)
    p.add_argument("--out", required=True)
    p.set_defaults(fn=cmd_plan)

    p = sp.add_parser("scaffold")
    p.add_argument("--fm", required=True)
    p.add_argument("--out", required=True)
    p.set_defaults(fn=cmd_scaffold)

    p = sp.add_parser("validate")
    p.add_argument("--config")
    p.add_argument("--lineage")
    p.set_defaults(fn=cmd_validate)

    p = sp.add_parser("test")
    p.add_argument("--fm", required=True)
    p.set_defaults(fn=cmd_test)

    p = sp.add_parser("run")
    p.add_argument("--lineage", required=True)
    p.set_defaults(fn=cmd_run)

    args = ap.parse_args()
    if not hasattr(args, "fn"):
        ap.print_help()
        sys.exit(1)
    args.fn(args)


if __name__ == "__main__":
    main()
