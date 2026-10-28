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
use datafusion::prelude::SessionContext;
use hudi_datafusion::HudiDataSource;
use hudi_test::variant::{variant_rows, variant_table_path};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn test_variant_table_scan_preserves_values_and_extensions() {
    for name in ["variant_cow", "variant_mor_avro", "variant_mor_spark"] {
        let root = variant_table_path(name);
        let provider = HudiDataSource::new(root.to_str().unwrap()).await.unwrap();
        let context = SessionContext::new();
        context
            .register_table("variants", Arc::new(provider))
            .unwrap();
        let batches = context
            .sql("SELECT id, v FROM variants WHERE id = 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            variant_rows(&batches),
            vec![(1, json!({"new_field":123,"updated":true}))],
            "{name}"
        );
    }
}
