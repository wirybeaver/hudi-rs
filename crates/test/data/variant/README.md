<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

-->

# Hudi Variant fixtures

The ZIP files are unmodified Spark-generated Hudi tables from
[apache/hudi at d1ec4d3b043d19487b6b8a2ab4bb44665863bc5a](https://github.com/apache/hudi/tree/d1ec4d3b043d19487b6b8a2ab4bb44665863bc5a/hudi-common/src/test/resources/variant_backward_compat).
They are Apache-2.0 licensed. The upstream README documents generation with Spark 4.0
and `TestVariantDataType.scala`.

- `variant_cow.zip`: COW; Variant children are ordered value, metadata.
- `variant_mor_avro.zip`: MOR with Avro data blocks; children are metadata, value.
- `variant_mor_spark.zip`: MOR produced using Spark records; children are value, metadata.

These table-version-9 fixtures contain an insert, an update of id 1 and deletion
of id 2. The first commit contains id 1 with `{"key":"value1","num":1}` and id 2
with `{"key":"value2","list":[1,2,3]}`. The final snapshot contains only id 1 with
`{"new_field":123,"updated":true}`. These expected values come from the producer's
SQL assertions, not from the Rust reader.

Tests read the complete Hudi table, including its persisted schema and timeline,
and compare decoded logical values. The MOR fixtures exercise base/log merging;
the historical read pins the pre-update values and the projection checks extension
metadata preservation.

`variant_shredded_mixed_cow.zip` is the upstream Spark 4.1 fixture: ids 1–2 have
a shredded string `key`, id 3 has an integer residual, and id 4 is unshredded.
Its table version is 10, which hudi-rs does not support. Tests consume the
unchanged Parquet base files and exercise Hudi schema reconciliation directly;
they do not relabel the table version or claim version-10 timeline/log support.
