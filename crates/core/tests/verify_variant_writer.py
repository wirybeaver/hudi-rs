#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""Read output retained by variant_write_tests using Spark 4.1 and Hudi 1.2.1.

Run with spark-submit and the matching Hudi bundle; pass the table directory.
"""

import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("hudi-rs-variant-writer-check")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    .config(
        "spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
    )
    .getOrCreate()
)
base = Path(sys.argv[1]).resolve()
jvm = spark.sparkContext._jvm
hconf = jvm.org.apache.hudi.storage.hadoop.HadoopStorageConfiguration(
    spark.sparkContext._jsc.hadoopConfiguration()
)
meta = (
    jvm.org.apache.hudi.common.table.HoodieTableMetaClient.builder()
    .setConf(hconf)
    .setBasePath(str(base))
    .setLoadActiveTimelineOnLoad(True)
    .build()
)
assert meta.getTableConfig().getTableVersion().versionCode() == 9
commits = sorted((base / ".hoodie/timeline").glob("*_*.commit"))
assert len(commits) == 2
for path in commits:
    datum = jvm.org.apache.avro.generic.GenericDatumReader()
    datum.setExpected(
        jvm.org.apache.hudi.avro.model.HoodieCommitMetadata.getClassSchema()
    )
    reader = jvm.org.apache.avro.file.DataFileReader(jvm.java.io.File(str(path)), datum)
    try:
        record = reader.next()
        assert not reader.hasNext()
        assert str(record.get("operationType")) == "INSERT"
        assert record.get("partitionToWriteStats").size() == 1
    finally:
        reader.close()
frame = spark.read.format("hudi").load(str(base))
assert frame.schema["v"].dataType.simpleString() == "variant"
rows = (
    frame.selectExpr(
        "id",
        "to_json(v) AS payload",
        "v IS NULL AS sql_null",
        "is_variant_null(v) AS variant_null",
    )
    .orderBy("id")
    .collect()
)
actual = [
    (
        r.id,
        json.loads(r.payload) if r.payload is not None else None,
        r.sql_null,
        r.variant_null,
    )
    for r in rows
]
assert actual == [
    (1, None, True, False),
    (2, None, False, True),
    (3, {"key": "first", "n": 1, "a": [1, None]}, False, False),
    (4, {"key": "second", "n": "fallback"}, False, False),
    (5, ["x", None], False, False),
], actual
print(
    f"PASS: Java commit schemas and {len(rows)} logical rows, including shredded residuals"
)
spark.stop()
