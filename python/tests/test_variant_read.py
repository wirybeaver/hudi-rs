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

"""Variant extension metadata survives the Python Arrow binding."""

from pathlib import Path
from zipfile import ZipFile

import pyarrow as pa

from hudi import HudiReadOptions, HudiTable


def test_variant_mor_read_preserves_extension_and_payload(tmp_path):
    fixtures = Path(__file__).resolve().parents[2] / "crates/test/data/variant"
    with ZipFile(fixtures / "variant_mor_avro.zip") as archive:
        archive.extractall(tmp_path)
    table = HudiTable(str(tmp_path / "variant_mor_avro"))
    batches = table.read(HudiReadOptions(projection=["id", "v"]))
    result = pa.Table.from_batches(batches)
    assert result.column("id").to_pylist() == [1]
    field = result.schema.field("v")
    assert field.metadata[b"ARROW:extension:name"] == b"arrow.parquet.variant"
    value = result.column("v").to_pylist()[0]
    assert value["metadata"]
    assert value["value"]
