# Lakehouse FM Agent (Scaffold)

A reusable toolkit to consume ABAP FM extractor outputs (LINEAGE.txt, INTERFACE.txt),
orchestrate FM handlers in Databricks, and replace BW mechanics with Lakehouse patterns.

## What this contains
- `fmtool.py`: a lightweight CLI (plan/graph/scaffold/validate/test/run)
- `runtime/`: lineage parser, topological sort, registry, orchestrator, manifest stub
- `core/`: shared logic placeholders (alpha, currency, uom, dates, cleansing)
- `dims/`: dimension/master joins placeholders (e.g., material)
- `bw_replace/`: BW replacements (DSO MERGE pattern, logsys mapping)
- `conf/`: example YAML configs for table names and rules
- `samples/`: a sample LINEAGE file + Mermaid graph output

## Quick start (Databricks)
1. Import this folder as a Repo or upload as a workspace directory.
2. Ensure reference tables (FX, UoM, calendar, etc.) are available in Unity Catalog.
3. Edit `conf/tables.yml` and `conf/rules.yml` to match your environment.
4. Run: `python fmtool.py graph --lineage samples/sample_LINEAGE.txt --out samples/graph.mmd`
5. Run: `python fmtool.py plan --lineage samples/sample_LINEAGE.txt --plan-id demo_001 --out samples/manifest.json`
6. Run: `python fmtool.py run --lineage samples/sample_LINEAGE.txt`

> NOTE: In real pipelines, handlers operate on Spark DataFrames and Delta tables.
> This scaffold focuses on wiring, parsing, and the replacement patterns.
