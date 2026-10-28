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

/// Detect physical shredding in an Avro schema before reader-schema resolution.
pub(crate) fn has_shredded_variant(schema: &apache_avro::Schema) -> bool {
    use apache_avro::Schema;
    match schema {
        Schema::Record(record) => {
            (record
                .attributes
                .get("logicalType")
                .and_then(serde_json::Value::as_str)
                == Some("variant")
                && record.fields.iter().any(|f| f.name == "typed_value"))
                || record
                    .fields
                    .iter()
                    .any(|f| has_shredded_variant(&f.schema))
        }
        Schema::Array(array) => has_shredded_variant(&array.items),
        Schema::Map(map) => has_shredded_variant(&map.types),
        Schema::Union(union) => union.variants().iter().any(has_shredded_variant),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_group::reader_v2::output_converter::{OutputConverter, ProjectionConverter};
    use arrow_array::cast::AsArray;
    use arrow_array::{
        Array, ArrayRef, ListArray, MapArray, RecordBatch, StringArray, StructArray,
    };
    use arrow_buffer::OffsetBuffer;
    use parquet::variant::{VariantArray, json_to_variant, shred_variant};
    use std::sync::Arc;

    #[test]
    fn test_shredded_variants_in_lists_and_maps_keep_nulls_and_residuals() {
        let json: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"x":7,"keep":["a",null]}"#),
            Some(r#"{"x":"residual","keep":true}"#),
            Some("null"),
            None,
        ]));
        let plain = json_to_variant(&json).unwrap();
        let physical = shred_variant(
            &plain,
            &DataType::Struct(vec![Field::new("x", DataType::Int64, true)].into()),
        )
        .unwrap();
        let wrap = |array: VariantArray| {
            let field = Arc::new(array.field("element"));
            let values: ArrayRef = Arc::new(array.into_inner());
            let list: ArrayRef = Arc::new(ListArray::new(
                field,
                OffsetBuffer::new(vec![0, 4].into()),
                values.clone(),
                None,
            ));
            let entries = StructArray::new(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", values.data_type().clone(), true)
                        .with_extension_type(VariantType),
                ]
                .into(),
                vec![
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                    values,
                ],
                None,
            );
            let entry_field = Arc::new(Field::new("key_value", entries.data_type().clone(), false));
            let map: ArrayRef = Arc::new(MapArray::new(
                entry_field,
                OffsetBuffer::new(vec![0, 4].into()),
                entries,
                None,
                false,
            ));
            RecordBatch::try_from_iter(vec![("items", list), ("attributes", map)]).unwrap()
        };
        let expected = wrap(plain.clone());
        let source = wrap(physical);
        let converter = ProjectionConverter::new(&expected.schema());
        let result = converter.apply(source).unwrap();
        let list_values = result.column(0).as_list::<i32>().values();
        let map_values = result.column(1).as_map().values();
        for column in [list_values, map_values] {
            let actual = VariantArray::try_new(column).unwrap();
            assert!(actual.is_null(3));
            assert!(!actual.is_null(2));
            let first = actual.try_value(0).unwrap();
            let object = first.as_object().unwrap();
            assert_eq!(object.get("x").unwrap().as_int64(), Some(7));
            let keep = object.get("keep").unwrap();
            let list = keep.as_list().unwrap();
            assert_eq!(list.get(0), Some(parquet::variant::Variant::from("a")));
            assert_eq!(list.get(1), Some(parquet::variant::Variant::Null));
            let second = actual.try_value(1).unwrap();
            let object = second.as_object().unwrap();
            assert_eq!(object.get("x").unwrap().as_string(), Some("residual"));
            assert_eq!(object.get("keep").unwrap().as_boolean(), Some(true));
            assert_eq!(
                actual.try_value(2).unwrap(),
                parquet::variant::Variant::Null
            );
        }
    }
    #[test]
    fn test_corrupt_shredded_variant_returns_a_field_error() {
        use arrow_array::{BinaryArray, Int64Array, new_null_array};
        let fields = vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("typed_value", DataType::Int64, true),
        ];
        let array: ArrayRef = Arc::new(StructArray::new(
            fields.clone().into(),
            vec![
                Arc::new(BinaryArray::from_vec(vec![&[255_u8][..]])),
                new_null_array(&DataType::Binary, 1),
                Arc::new(Int64Array::from(vec![42])),
            ],
            None,
        ));
        let source = RecordBatch::try_from_iter(vec![("v", array)]).unwrap();
        let logical = Field::new("v", DataType::Struct(fields[..2].to_vec().into()), true)
            .with_extension_type(VariantType);
        let target = Arc::new(arrow_schema::Schema::new(vec![logical]));
        let error =
            crate::schema::batch_evolution::project_batch_to_schema(&source, &target).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Failed to reconstruct Variant 'v'"),
            "{error}"
        );
    }
}
