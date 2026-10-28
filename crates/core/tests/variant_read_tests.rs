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
use hudi_core::table::{QueryType, ReadOptions, Table};
use hudi_test::variant::{variant_rows, variant_table_path};
use serde_json::json;

#[tokio::test]
async fn test_spark_variant_tables_snapshot_time_travel_and_projection() {
    for name in ["variant_cow", "variant_mor_avro", "variant_mor_spark"] {
        let root = variant_table_path(name);
        let table = Table::new(root.to_str().unwrap()).await.unwrap();
        let batch = table
            .read(&ReadOptions::new().with_projection(["id", "v"]))
            .await
            .unwrap();
        assert_eq!(
            variant_rows(&batch),
            vec![(1, json!({"new_field": 123, "updated": true}))],
            "{name} snapshot"
        );
        let commits = &table.get_timeline().completed_commits;
        let incremental = table
            .read(
                &ReadOptions::new()
                    .with_query_type(QueryType::Incremental)
                    .with_start_timestamp(commits[0].completion_timestamp.as_ref().unwrap())
                    .with_end_timestamp(commits[1].completion_timestamp.as_ref().unwrap())
                    .with_projection(["id", "v"]),
            )
            .await
            .unwrap();
        assert_eq!(
            variant_rows(&incremental),
            vec![(1, json!({"new_field":123,"updated":true}))],
            "{name} incremental update"
        );
        let first = &commits[0].timestamp;
        let initial = table
            .read(&ReadOptions::new().with_as_of_timestamp(first))
            .await
            .unwrap();
        assert_eq!(
            variant_rows(&initial),
            vec![
                (1, json!({"key":"value1", "num":1})),
                (2, json!({"key":"value2", "list":[1,2,3]}))
            ],
            "{name} initial commit"
        );
    }
}

#[tokio::test]
async fn test_mixed_spark_variants_reconstruct_before_projection() {
    use arrow_schema::{DataType, Schema};
    use hudi_core::file_group::base_file::parquet::ParquetBaseFileReader;
    use hudi_core::file_group::base_file::reader::{BaseFileReadOptions, BaseFileReader};
    use hudi_core::schema::batch_evolution::project_batch_to_schema;
    use hudi_core::storage::Storage;
    use std::sync::Arc;
    let root = variant_table_path("variant_shredded_mixed_cow");
    let storage = Storage::new(
        Arc::new(Default::default()),
        Arc::new(hudi_core::config::HudiConfigs::new([(
            hudi_core::config::table::HudiTableConfig::BasePath.as_ref(),
            root.to_str().unwrap(),
        )])),
    )
    .unwrap();
    let reader = ParquetBaseFileReader::new(storage);
    let mut reconstructed = Vec::new();
    let mut shredded_files = 0;
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|s| s != "parquet") {
            continue;
        }
        let batch = reader
            .read_data(
                path.file_name().unwrap().to_str().unwrap(),
                BaseFileReadOptions::default(),
            )
            .await
            .unwrap();
        let schema = batch.schema();
        let DataType::Struct(fields) = schema.field_with_name("v").unwrap().data_type() else {
            panic!("variant must be a struct")
        };
        shredded_files += usize::from(fields.iter().any(|f| f.name() == "typed_value"));
        let logical: arrow_schema::Fields = fields
            .iter()
            .filter(|f| f.name() != "typed_value")
            .cloned()
            .collect();
        let target = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .map(|f| {
                    if f.name() == "v" {
                        Arc::new(
                            f.as_ref()
                                .clone()
                                .with_data_type(DataType::Struct(logical.clone()))
                                .with_extension_type(parquet::variant::VariantType),
                        )
                    } else {
                        f.clone()
                    }
                })
                .collect::<Vec<_>>(),
        ));
        reconstructed.push(project_batch_to_schema(&batch, &target).unwrap());
    }
    assert_eq!(shredded_files, 1);
    assert_eq!(
        variant_rows(&reconstructed),
        vec![
            (1, json!({"key":"k1"})),
            (2, json!({"key":"k2"})),
            (3, json!({"key":3})),
            (4, json!({"key":"k4"})),
        ]
    );
}
