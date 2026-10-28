/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use arrow_array::{Array, Int32Array, RecordBatch};
use parquet::variant::{VariantArray, VariantType, variant_to_json};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// Path to a pinned Java/Spark-produced Variant table fixture.
pub fn variant_table_path(name: &str) -> PathBuf {
    let zip = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("data/variant/{name}.zip"));
    crate::extract_test_table(&zip).join(name)
}

/// Decode fixture rows to logical JSON while checking Variant extension metadata.
pub fn variant_rows(batches: &[RecordBatch]) -> Vec<(i32, Value)> {
    let mut rows = Vec::new();
    for batch in batches {
        batch
            .schema()
            .field_with_name("v")
            .unwrap()
            .try_extension_type::<VariantType>()
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let column = batch.column_by_name("v").unwrap();
        let variants = VariantArray::try_new(column).unwrap();
        for i in 0..variants.len() {
            if variants.is_null(i) {
                continue;
            }
            variants
                .try_value(i)
                .unwrap_or_else(|e| panic!("{e}: {column:?}"));
        }
        // The JSON kernel expects metadata/value in that order and Binary storage.
        let canonical: arrow_array::ArrayRef = std::sync::Arc::new(arrow_array::StructArray::new(
            vec![
                arrow_schema::Field::new("metadata", arrow_schema::DataType::Binary, false),
                arrow_schema::Field::new("value", arrow_schema::DataType::Binary, true),
            ]
            .into(),
            vec![
                arrow_cast::cast(variants.metadata_column(), &arrow_schema::DataType::Binary)
                    .unwrap(),
                arrow_cast::cast(variants.value_column(), &arrow_schema::DataType::Binary).unwrap(),
            ],
            variants.nulls().cloned(),
        ));
        let values = variant_to_json(&canonical).unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                if values.is_null(row) {
                    Value::Null
                } else {
                    serde_json::from_str(values.value(row)).unwrap()
                },
            ));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}
