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
use arrow_schema::{DataType, Field};
use parquet::variant::VariantType;

/// Whether a field or one of its nested children carries the Variant extension.
pub fn contains_variant(field: &Field) -> bool {
    if field.has_valid_extension_type::<VariantType>() {
        return true;
    }
    match field.data_type() {
        DataType::Struct(fields) => fields.iter().any(|f| contains_variant(f)),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => contains_variant(f),
        DataType::Union(fields, _) => fields.iter().any(|(_, f)| contains_variant(f)),
        _ => false,
    }
}
