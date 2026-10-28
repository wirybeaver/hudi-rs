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
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hudi_core::config::{HudiConfigs, table::HudiTableConfig};
use hudi_core::storage::Storage;
use hudi_core::table::{QueryType, ReadOptions, Table, writer::CowWriter};
use hudi_test::variant::variant_rows;
use parquet::variant::{Variant, VariantArray, json_to_variant};
use serde_json::json;
use std::path::Path;
use std::sync::Arc;

const SCHEMA: &str = r#"{"type":"record","name":"variants","namespace":"example","doc":"雪\nquoted \"text\"","fields":[{"name":"id","type":"int"},{"name":"v","type":["null",{"type":"record","name":"variant","logicalType":"variant","fields":[{"name":"metadata","type":"bytes"},{"name":"value","type":"bytes"}]}],"default":null}]}"#;

fn storage(path: &Path) -> Arc<Storage> {
    Storage::new(
        Arc::new(Default::default()),
        Arc::new(HudiConfigs::new([(
            HudiTableConfig::BasePath.as_ref(),
            path.to_str().unwrap(),
        )])),
    )
    .unwrap()
}

fn batch(ids: Vec<i32>, values: Vec<Option<&str>>) -> RecordBatch {
    let strings: ArrayRef = Arc::new(StringArray::from(values));
    let variants = json_to_variant(&strings).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        variants.field("v"),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(variants.into_inner()),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_variant_cow_append_reopen_and_shredded_round_trip() {
    let temp = tempfile::tempdir().unwrap();
    let store = storage(temp.path());
    let mut writer = CowWriter::create(store.clone(), "variants", SCHEMA, "id")
        .await
        .unwrap();
    let empty = Table::new(temp.path().to_str().unwrap()).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&empty.get_schema_in_avro_str().await.unwrap())
            .unwrap(),
        serde_json::from_str::<serde_json::Value>(SCHEMA).unwrap()
    );
    writer
        .append(
            batch(
                vec![1, 2, 3],
                vec![
                    None,
                    Some("null"),
                    Some(r#"{"key":"first","n":1,"a":[1,null]}"#),
                ],
            ),
            "20260101000000000",
            "20260101000000001",
        )
        .await
        .unwrap();
    let mut writer = CowWriter::open(store.clone())
        .await
        .unwrap()
        .with_variant_shredding(
            "v",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, true),
                    Field::new("n", DataType::Int64, true),
                ]
                .into(),
            ),
        )
        .unwrap();
    writer
        .append(
            batch(
                vec![4, 5],
                vec![
                    Some(r#"{"key":"second","n":"fallback"}"#),
                    Some(r#"["x",null]"#),
                ],
            ),
            "20260101000001000",
            "20260101000001001",
        )
        .await
        .unwrap();
    for file in store
        .list_files(None)
        .await
        .unwrap()
        .iter()
        .filter(|f| f.name.ends_with(".parquet"))
    {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            store.get_file_data(&file.name).await.unwrap(),
        )
        .unwrap();
        let field = reader
            .parquet_schema()
            .root_schema()
            .get_fields()
            .iter()
            .find(|f| f.name() == "v")
            .unwrap();
        assert_eq!(
            field.get_basic_info().logical_type_ref(),
            Some(&parquet::basic::LogicalType::variant(Some(1)))
        );
    }
    let table = Table::new(temp.path().to_str().unwrap()).await.unwrap();
    let records = table
        .read(&ReadOptions::new().with_projection(["id", "v"]))
        .await
        .unwrap();
    assert_eq!(
        variant_rows(&records),
        vec![
            (1, json!(null)),
            (2, json!(null)),
            (3, json!({"key":"first","n":1,"a":[1,null]})),
            (4, json!({"key":"second","n":"fallback"})),
            (5, json!(["x", null]))
        ]
    );
    for batch in &records {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let variants = VariantArray::try_new(batch.column_by_name("v").unwrap()).unwrap();
        for row in 0..batch.num_rows() {
            if ids.value(row) == 1 {
                assert!(variants.is_null(row));
            }
            if ids.value(row) == 2 {
                assert!(!variants.is_null(row));
                assert_eq!(variants.try_value(row).unwrap(), Variant::Null);
            }
        }
    }
    let incremental = table
        .read(
            &ReadOptions::new()
                .with_query_type(QueryType::Incremental)
                .with_start_timestamp("20260101000000001")
                .with_end_timestamp("20260101000001001"),
        )
        .await
        .unwrap();
    assert_eq!(
        variant_rows(&incremental),
        vec![
            (4, json!({"key":"second","n":"fallback"})),
            (5, json!(["x", null]))
        ]
    );
    let ordinary = batch(vec![6], vec![Some("42")]);
    let wrong_type = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(arrow_array::Int64Array::from(vec![6])) as ArrayRef,
        ),
        ("v", ordinary.column_by_name("v").unwrap().clone()),
    ])
    .unwrap();
    let error = writer
        .append(wrong_type, "20260101000002000", "20260101000002001")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("requires Int32, got Int64"),
        "{error}"
    );
    assert!(
        writer
            .append(
                batch(vec![6], vec![Some("42")]),
                "20260101000001000",
                "20260101000001001"
            )
            .await
            .is_err()
    );
    assert!(
        CowWriter::create(store, "variants", SCHEMA, "id")
            .await
            .is_err()
    );
    if std::env::var_os("HUDI_VARIANT_KEEP_OUTPUT").is_some() {
        println!("VARIANT_WRITER_OUTPUT={}", temp.keep().display());
    }
}

#[tokio::test]
async fn test_cow_failed_data_write_stays_invisible_and_never_overwrites() {
    let temp = tempfile::tempdir().unwrap();
    let store = storage(temp.path());
    let mut writer = CowWriter::create(store.clone(), "variants", SCHEMA, "id")
        .await
        .unwrap();
    writer
        .append(
            batch(vec![1], vec![Some("42")]),
            "20260101000000000",
            "20260101000000001",
        )
        .await
        .unwrap();
    let collision = temp
        .path()
        .join("20260101000001000-0_0-0-0_20260101000001000.parquet");
    std::fs::write(&collision, b"existing object").unwrap();
    assert!(
        writer
            .append(
                batch(vec![2], vec![Some("43")]),
                "20260101000001000",
                "20260101000001001"
            )
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&collision).unwrap(), b"existing object");
    let table = Table::new(temp.path().to_str().unwrap()).await.unwrap();
    assert_eq!(table.get_timeline().completed_commits.len(), 1);
    assert_eq!(
        variant_rows(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![(1, json!(42))]
    );
    let mut reopened = CowWriter::open(store).await.unwrap();
    reopened
        .append(
            batch(vec![3], vec![Some("44")]),
            "20260101000002000",
            "20260101000002001",
        )
        .await
        .unwrap();
    let table = Table::new(temp.path().to_str().unwrap()).await.unwrap();
    assert_eq!(
        variant_rows(&table.read(&ReadOptions::new()).await.unwrap()),
        vec![(1, json!(42)), (3, json!(44))]
    );
}
