# DataFrame (experimentEngine/DataFrame13.java)

DataFrame is an experimental columnar data engine implemented in Java. The primary implementation for this repository is `experimentEngine/DataFrame13.java`. This engine demonstrates core column-store concepts including dictionary encoding, segment-oriented storage, bitpacked encoding, predicate evaluation (predicate AST), view-based filtering/projection, CSV import/export, persistence (per-column files + serialized metadata), and fast joins using pre-built join indices.

This README documents how to build and run the demo, core API surfaces, examples, implementation details, limitations, and development notes. Use this as a starting point for experimenting and extending the engine.

---

## Table of contents

- [Overview](#overview)  
- [Features](#features)  
- [Requirements](#requirements)  
- [Build & Run (demo)](#build--run-demo)  
- [Quickstart](#quickstart)  
  - [Load CSV (FROMCSV)](#load-csv-fromcsv)  
  - [Persist & Reload](#persist--reload)  
  - [Filter (WHERE)](#filter-where)  
  - [Project (SELECT)](#project-select)  
  - [Export CSV (TOCSV)](#export-csv-tocsv)  
  - [Join (INNER/LEFT/OUTER + DFFROMJOIN)](#join-innerleftouter--dffromjoin)  
- [Core API (reference)](#core-api-reference)  
- [Implementation details](#implementation-details)  
- [Limitations & gotchas](#limitations--gotchas)  
- [Performance tips & best practices](#performance-tips--best-practices)  
- [Development & TODOs](#development--todos)  
- [Future development](#future-development)  
- [Contributing](#contributing)

---

## Overview

`DataFrame13.java` is an educational prototype of a columnar engine. It supports:

- Dynamic schema inference from CSV (FROMCSV)
- Per-column dictionary encoding
- Fixed-size segments with per-segment metadata (min/max/sum, bitmap index)
- BITPACK encoding for per-segment storage; simple RLE/CONSTANT decoders present
- Per-segment bitmap indices for fast equality predicates
- Predicate AST (COMPARE, AND, OR, NOT) evaluated per segment (parallel)
- SELECT projection and WHERE filtering producing views (rowIndexView)
- Per-column persistent files (`<column>.col`) and serialized metadata (`metadata.dat`)
- Fast joins using pre-built join indices (dictId → global row indices)
- CSV export (TOCSV)

This is a demonstration engine (research/prototype), not production-ready.

---

## Features

- Column-oriented layout with per-column dictionary (value → dict id).
- Numeric dictionary optimization: numeric dictionaries can be materialized as `int[]` for fast numeric comparisons.
- Segment-level metadata and bitmap index for efficient EQ filtering and pruning via numeric min/max.
- BITPACK encoding of dict ids (LSB-first) for compact storage; decoders for RLE and CONSTANT are available.
- Memory-mapped column files for efficient read access after persistence.
- Predicate AST supporting AND/OR/NOT and comparison operators.
- View model: WHERE returns a filtered view (IntList rowIndexView), SELECT returns a projection view.
- Fast joins when right-hand table stored with `buildJoinIndex=true` during finalize/store.

---

## Requirements

- Java 8+ (uses java.nio, ExecutorService, java.time).
- No external libraries required.

---

## Build & Run (demo)

From the repository root:

Compile:
```
javac -d out experimentEngine/DataFrame13.java
```

Run the demo:
```
java -cp out experimentEngine.DataFrame13
```

The demo will:
- Create `./data_df13_output`
- Generate example CSVs (`customers.csv`, `orders.csv`)
- Demonstrate loading (FROMCSV), persisting (finalizeStore/save), reloading (FROM), filtering (WHERE), projection (SELECT), joining, and exporting (TOCSV)
- Print statistics and timings

---

## Quickstart

Snippets below show typical flows. These are Java snippets using the classes inside `DataFrame13.java`.

### Load CSV (FROMCSV)
```java
DataFrame13 df = DataFrame13.FROMCSV(Paths.get("customers.csv"), ",", 20);
```
Notes:
- `delimiter` is used with `String.split`.
- `segmentSize` controls rows per segment and affects pruning/metadata granularity.
- `FROMCSV` uses a naive CSV parser and reads the whole file into memory; call `finalizeInMemory()` or `finalizeStore()` before running segment-based queries.

Optionally enforce inferred type:
```java
    df.columns.get("customerID").inferredType = Integer.class;
```

### Persist & Reload
Write per-column `.col` files and optional join index:
```java
df.finalizeStore(Paths.get("./store/customers"), true); // true = build join index
df.save(Paths.get("./store/customers")); // writes metadata.dat
```

Reload:
```java
DataFrame13 loaded = DataFrame13.FROM(Paths.get("./store/customers"));
```

### Filter (WHERE)
Construct predicate AST and filter:
```java
COMPARE p1 = new COMPARE("customerID", 101, OPERATOR.EQ);
COMPARE p2 = new COMPARE("customerID", 108, OPERATOR.GT);
LOGIC predicate = new OR(p1, p2);
DataFrame13 filtered = loaded.WHERE(predicate);
```
- WHERE chooses a primary column from predicate columns, evaluates per segment (parallel), and returns a view.

### Project (SELECT)
```java
DataFrame13 projected = filtered.SELECT("name", "customerID");
```
- SELECT returns a new DataFrame view sharing the same Column objects and rowIndexView.

### Export CSV (TOCSV)
```java
projected.TOCSV(Paths.get("filtered_customers.csv"), new String[]{"customerID","name"}, ",");
```
- Fields containing the delimiter are quoted and quotes are doubled.

### Join (INNER/LEFT/OUTER + DFFROMJOIN)
Ensure the right-side table was persisted with `buildJoinIndex=true`.

Inner join:
```java
DataFrame13.MATCHED matches = DataFrame13.INNERMATCH(filtered, "customerID", dfB_loaded, "customerKey");
DataFrame13 joined = DataFrame13.DFFROMJOIN(matches, filtered, dfB_loaded);
joined.SHOW(10);
```

Left join:
```java
DataFrame13.MATCHED leftMatches = DataFrame13.LEFTMATCH(filtered, "customerID", dfB_loaded, "customerKey");
DataFrame13 leftJoined = DataFrame13.DFFROMJOIN(leftMatches, filtered, dfB_loaded);
```

Outer join:
```java
DataFrame13.MATCHED outerMatches = DataFrame13.OUTERMATCH(filtered, "customerID", dfB_loaded, "customerKey");
DataFrame13 outerJoined = DataFrame13.DFFROMJOIN(outerMatches, filtered, dfB_loaded);
```

`MATCHED.matches` contains `Pair<Integer,Integer>` pairs where `.left` is viewIndexA and `.right` is viewIndexB. `-1` denotes a null/no-match for outer joins.

---

## Core API (reference)

Main constructor:
- `DataFrame13(int segmentSize)`

I/O:
- `void save(Path storageDir)` — serialize metadata to `metadata.dat`
- `static DataFrame13 FROM(Path storageDir)` — load metadata and memory-map column files
- `List<Path> finalizeStore(Path storageDir, boolean buildJoinIndex)` — write `.col` files and optionally build join index
- `void finalizeInMemory()` — build segment headers from rawIds in memory (no files)

CSV:
- `static DataFrame13 FROMCSV(Path csvPath, String delimiter, int segmentSize)`
- `void TOCSV(Path outputPath, String[] columnNames, String delimiter)`

Query & projection:
- `DataFrame13 WHERE(LOGIC predicate)` — returns view filtered by predicate AST
- `DataFrame13 SELECT(String... columnNames)` — projection view
- `Map<String, Object> getRowData(int viewIndex, String... columnNames)` — fetch row values
- `void SHOW(int rowsToShow)` — pretty print rows
- `void explain()` — print engine and column stats

Predicate AST:
- `LOGIC`, `COMPARE`, `AND`, `OR`, `NOT`
- `OPERATOR { EQ, LT, LE, GT, GE, NE }`

Joins:
- `MATCHED INNERMATCH(...)`
- `MATCHED LEFTMATCH(...)`
- `MATCHED OUTERMATCH(...)`
- `static DataFrame13 DFFROMJOIN(MATCHED result, DataFrame13 dfA, DataFrame13 dfB)`

---

## Implementation details

- Dictionary encoding: columns maintain `dict` (List<Object>) and `dictMap` (Map<Object,Integer>). Rows are stored as dict IDs (`rawIds`) during ingestion.
- Numeric optimization: `finalizeNumericDict()` converts dict entries into `int[] numericDictValues` for fast numeric comparisons; string entries that parse as numbers can be converted, otherwise fallback to `0`.
- Segmentation: rows split into fixed-size `segmentSize` blocks; `SegmentHeader` stores per-segment metadata (count, bits, min, max, sum, file offset/length, bitmapIndex).
- Encoding: BITPACK is the active writer (LSB-first); `BitReader` decodes BITPACK. Simple RLE/CONSTANT decoders exist for decoding, but writer currently uses BITPACK.
- Bitmap index: per-segment `BitmapIndex` maps dictId → BitSet of local-row matches; used for EQ predicate evaluation.
- Persistence: `finalizeStore()` writes `.col` files and memory-maps them back into `Column.mapped` buffers. `save()` writes serializable metadata (`SerializableState`) to `metadata.dat`. `FROM()` reads metadata and remaps column files.
- Views: WHERE produces a DataFrame view with `rowIndexView` (IntList of global row indices). SELECT returns a projection view containing only chosen columns.
- Joins: `preBuiltJoinIndex` created in `finalizeStore(..., true)` maps dictId → global row indices for fast joins on the right-hand table.

---

## Limitations & gotchas

- CSV handling:
  - `FROMCSV` uses `String.split(delimiter)` and `Files.readAllLines`. It does NOT handle quoted fields, escaped delimiters, or multiline records. For robust CSV ingestion, replace with a proper parser (e.g., OpenCSV, Jackson CSV, or Apache Commons CSV).
  - `FROMCSV` reads the entire file into memory; large files may cause OOM.

- Encodings:
  - Writer currently only emits BITPACK encoding. RLE/CONSTANT writing heuristics are not implemented.

- Numeric conversions:
  - `finalizeNumericDict()` converts non-parseable string dict entries to `0`. This may be undesirable for dirty data.

- MappedByteBuffer management:
  - Unmapping is not explicit. `close()` nulls mapped references and relies on JVM GC to unmap. For deterministic unmapping, platform-specific techniques are required.

- View sharing:
  - Views share `Column` objects. Do not mutate columns while views are active.

- Index mapping:
  - `globalRowIndexToSegIndex` maps `columnName:globalRow` → segment index and must be consistent; inconsistencies will raise exceptions.

---

## Performance tips & best practices

- Persist and reload (finalizeStore + save, then FROM) to benefit from memory-mapped `.col` files.
- Tune `segmentSize`:
  - Smaller segments increase pruning effectiveness and parallelism but increase metadata count.
  - Larger segments reduce metadata overhead but may increase decode cost per predicate.
- Favor equality predicates on dictionary-encoded columns to leverage bitmap indices (avoid full segment decode).
- For join-heavy workloads, persist the right-hand table with `buildJoinIndex=true` to create `preBuiltJoinIndex`.
- Avoid many random single-row reads: `getRowData()` decodes segments on demand and repeated decoding is costly.

---

## Development & TODOs

Ideas and recommended improvements:
- Replace naive CSV logic with a streaming, RFC-compliant CSV parser.
- Implement RLE and CONSTANT encoders and heuristics to select per-segment encoding.
- Add deterministic `MappedByteBuffer` unmap support for explicit resource cleanup.
- Improve numeric storage to preserve full precision (e.g., `double[]`, `long[]`, or specialized encoding instead of casting to `int`).
- Add unit tests for edge cases and unexpected CSV inputs.
- Consider storing `SegmentHeader` metadata in the `.col` file header for co-location and integrity checks.

---

## Future development

The project will evolve toward lightweight capabilities focused on:

- Graph processing (lightweight graph algorithms and traversal on encoded data)
- Orchestration (simple task/workflow orchestration for ETL and analytics pipelines)
- BI (basic business-intelligence features and aggregation primitives)
- ETL (lightweight extract-transform-load utilities integrated with the engine)

These directions aim to keep the engine lightweight while enabling richer data-processing scenarios.

---

## Contributing

- Open issues describing bugs or feature requests.
- Create PRs against the `main` branch (or a named feature branch).
- Suggested contributions:
  - Robust CSV ingestion
  - RLE/CONSTANT encoding writers
  - Explicit `MappedByteBuffer` unmap helper
  - Unit tests and CI integration
  - Performance and correctness improvements
