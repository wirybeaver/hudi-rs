<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Native COW append writer

`hudi::table::writer::CowWriter` creates and reopens nonpartitioned Hudi version 9
COW Parquet tables. It supports INSERT/append with new file groups, a fixed Avro
logical schema, and a single Utf8 or integer record-key field. Each append accepts
one Arrow `RecordBatch`; the encoded Parquet file is buffered in memory, so batch
size bounds the writer's memory use.

Create a `Storage` using the same URI and object-store options as the readers,
then call `CowWriter::create(storage, table_name, avro_schema, record_key)`.
`CowWriter::open(storage)` resumes a compatible table. Input batches must supply
exactly the logical columns and primitive types, excluding `_hoodie_*`; the writer supplies the five
Hudi metadata columns. Keys must be non-null and nonempty. Schema evolution,
partitioning, metadata-table indexes, upserts, deduplication, deletes, MOR writes
and table services are not supported by this append interface.

```rust
writer.append(batch, "20260101000000000", "20260101000000001").await?;
```

Timestamps are caller-supplied UTC `yyyyMMddHHmmssSSS` instants. Requests must
strictly increase, including past failed/pending requests observed on reopen;
completion cannot precede request. An empty batch is a no-op.

Variant columns use the Arrow `arrow.parquet.variant` extension. Unshredded output
is the default. To shred a top-level Variant column, configure
`writer.with_variant_shredding("v", target_type)`, where `target_type` is an Arrow
primitive, Struct, or List describing the desired typed values. Mismatching
values remain in the residual. The logical table/commit schema stays unchanged,
and Arrow writes the standard Parquet VARIANT annotation. Reading these files
requires a Parquet implementation that understands that annotation (Parquet Java
1.16 or newer); older Spark 4.0 bundles do not.

The writer creates requested and inflight markers before writing data, then
publishes a completed Avro OCF commit after the data file succeeds. All objects
use create-only writes. A failed append leaves pending markers and orphan files
in place; it neither overwrites objects nor publishes a completion for a failed
data write. Readers gate visibility on the completed timeline.

This is a single-writer contract, not distributed locking or retry idempotency.
Do not concurrently mutate the table with another writer. A timeout while
publishing completion is ambiguous: inspect the timeline before retrying.
Open a fresh `Table` after appending to observe its updated timeline.

The integration test can retain its generated table with
`HUDI_VARIANT_KEEP_OUTPUT=1`; it prints `VARIANT_WRITER_OUTPUT`. Run
[`verify_variant_writer.py`](../crates/core/tests/verify_variant_writer.py) against
that directory with Spark 4.1 and the matching Hudi bundle. The verifier resolves
commit metadata against Java's generated Avro schema and checks logical values,
shredded residuals, SQL null and Variant null through a native Hudi read.
