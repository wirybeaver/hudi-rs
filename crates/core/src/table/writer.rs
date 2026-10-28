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
//! Single-writer append of Arrow batches to nonpartitioned Hudi COW tables.

use crate::config::table::HudiTableConfig;
use crate::config::util::parse_data_for_options;
use crate::error::{CoreError, Result};
use crate::metadata::commit::{HoodieCommitMetadata, HoodieWriteStat};
use crate::metadata::meta_field::MetaField;
use crate::schema::batch_evolution::project_batch_to_schema;
use crate::schema::resolver::avro_json_to_arrow_schema;
use crate::storage::Storage;
use crate::storage::error::StorageError;
use crate::storage::util::join_url_segments;
use apache_avro::schema::AvroSchema;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, extension::ExtensionType};
use futures::TryStreamExt;
use object_store::{PutMode, PutOptions};
use parquet::arrow::{ArrowSchemaConverter, ArrowWriter, arrow_writer::ArrowWriterOptions};
use parquet::basic::LogicalType;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::{SchemaDescriptor, Type, TypePtr};
use parquet::variant::{VariantArray, VariantType, shred_variant};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// A single-writer, nonpartitioned, table-version-9 COW INSERT/append writer.
///
/// Each append writes one new file group and publishes its completed instant last.
/// Callers supply strictly increasing UTC request timestamps and completion times
/// (`yyyyMMddHHmmssSSS`). This writer does not deduplicate keys, coordinate other
/// writers, evolve schemas, maintain metadata indexes, or perform table services.
/// A failed completion request may have committed remotely: inspect the timeline
/// before retrying. Existing objects are never overwritten or removed.
///
/// Input batches must supply exactly the logical schema's columns. Variant data
/// uses Arrow's extension-marked Struct representation. One batch is encoded in
/// memory per append; choose batch sizes to bound that memory.
pub struct CowWriter {
    storage: Arc<Storage>,
    schema: SchemaRef,
    avro_schema: String,
    record_key: String,
    last_requested: Option<String>,
    shredding: BTreeMap<String, DataType>,
}

impl CowWriter {
    /// Create a new table. `storage` supplies URI routing and cloud credentials.
    ///
    /// The record key must be one Utf8, Int32 or Int64 column. Table
    /// creation requires an empty location. Only this writer may
    /// mutate the table while an instance is open.
    pub async fn create(
        storage: Arc<Storage>,
        table_name: &str,
        avro_schema: &str,
        record_key: &str,
    ) -> Result<Self> {
        if table_name.is_empty() || table_name.trim() != table_name {
            return Err(CoreError::InvalidValue(
                "Table name must be nonempty with no surrounding whitespace".into(),
            ));
        }
        let writer = Self::from_schema(storage, avro_schema, record_key, None)?;
        let prefix = object_store::path::Path::from_url_path(writer.storage.base_url.path())
            .map_err(StorageError::from)?;
        if writer
            .storage
            .object_store
            .list(Some(&prefix))
            .try_next()
            .await
            .map_err(StorageError::from)?
            .is_some()
        {
            return Err(CoreError::InvalidValue(
                "New COW table location must be empty".into(),
            ));
        }

        let properties = BTreeMap::from([
            (
                HudiTableConfig::TableName.as_ref().to_string(),
                table_name.to_string(),
            ),
            (
                HudiTableConfig::TableType.as_ref().into(),
                "COPY_ON_WRITE".into(),
            ),
            (HudiTableConfig::TableVersion.as_ref().into(), "9".into()),
            (
                HudiTableConfig::TimelineLayoutVersion.as_ref().into(),
                "2".into(),
            ),
            (
                HudiTableConfig::TimelineTimezone.as_ref().into(),
                "UTC".into(),
            ),
            (
                HudiTableConfig::BaseFileFormat.as_ref().into(),
                "PARQUET".into(),
            ),
            (
                HudiTableConfig::RecordKeyFields.as_ref().into(),
                record_key.into(),
            ),
            (
                HudiTableConfig::PopulatesMetaFields.as_ref().into(),
                "true".into(),
            ),
            (
                HudiTableConfig::CreateSchema.as_ref().into(),
                writer.avro_schema.clone(),
            ),
            ("hoodie.table.initial.version".into(), "9".into()),
            ("hoodie.table.format".into(), "native".into()),
            ("hoodie.timeline.path".into(), "timeline".into()),
            ("hoodie.timeline.history.path".into(), "history".into()),
            (
                HudiTableConfig::KeyGeneratorType.as_ref().into(),
                "NON_PARTITION".into(),
            ),
            (
                "hoodie.record.merge.mode".into(),
                "COMMIT_TIME_ORDERING".into(),
            ),
        ]);
        let mut crc = flate2::Crc::new();
        crc.update(format!(".{table_name}").as_bytes());
        let mut bytes = String::new();
        for (key, value) in &properties {
            bytes.push_str(&format!("{key}={}\n", escape_property(value)));
        }
        bytes.push_str(&format!("hoodie.table.checksum={}\n", crc.sum()));
        writer
            .put_new(".hoodie/hoodie.properties", bytes.into_bytes())
            .await?;
        Ok(writer)
    }

    /// Reopen a compatible COW append table. Schema evolution and metadata
    /// indexes are rejected so an append cannot silently leave an index stale.
    pub async fn open(storage: Arc<Storage>) -> Result<Self> {
        let properties = parse_data_for_options(
            &storage.get_file_data(".hoodie/hoodie.properties").await?,
            "=",
        )?;
        crate::table::validation::validate_configs(&crate::config::HudiConfigs::new(
            properties.iter(),
        ))?;
        for (key, expected) in [
            (HudiTableConfig::TableType.as_ref(), "COPY_ON_WRITE"),
            (HudiTableConfig::TableVersion.as_ref(), "9"),
            (HudiTableConfig::TimelineLayoutVersion.as_ref(), "2"),
            (HudiTableConfig::TimelineTimezone.as_ref(), "UTC"),
            (HudiTableConfig::BaseFileFormat.as_ref(), "PARQUET"),
            (HudiTableConfig::PopulatesMetaFields.as_ref(), "true"),
            ("hoodie.timeline.path", "timeline"),
            ("hoodie.table.format", "native"),
            ("hoodie.record.merge.mode", "COMMIT_TIME_ORDERING"),
            (HudiTableConfig::KeyGeneratorType.as_ref(), "NON_PARTITION"),
        ] {
            if properties.get(key).map(String::as_str) != Some(expected) {
                return Err(CoreError::Unsupported(format!(
                    "COW append requires {key}={expected}"
                )));
            }
        }
        for key in [
            HudiTableConfig::PartitionFields.as_ref(),
            "hoodie.table.metadata.partitions",
            "hoodie.table.metadata.partitions.inflight",
            "hoodie.bootstrap.base.path",
        ] {
            if properties.get(key).is_some_and(|value| !value.is_empty()) {
                return Err(CoreError::Unsupported(format!(
                    "COW append does not support {key}"
                )));
            }
        }
        let schema = properties
            .get(HudiTableConfig::CreateSchema.as_ref())
            .ok_or_else(|| {
                CoreError::Schema("COW append requires the persisted create schema".into())
            })?;
        let key = properties
            .get(HudiTableConfig::RecordKeyFields.as_ref())
            .ok_or_else(|| CoreError::Schema("COW append requires a record key field".into()))?;
        let instants = storage.list_files(Some(".hoodie/timeline")).await?;
        let last = instants
            .iter()
            .filter_map(|file| file.name.get(..17))
            .filter(|name| name.bytes().all(|c| c.is_ascii_digit()))
            .max()
            .map(str::to_string);
        if let Some(latest) = instants
            .iter()
            .filter(|f| f.name.ends_with(".commit"))
            .max_by_key(|f| &f.name)
        {
            let metadata = HoodieCommitMetadata::from_avro_bytes(
                &storage
                    .get_file_data(&format!(".hoodie/timeline/{}", latest.name))
                    .await?,
            )?;
            let latest_schema = metadata
                .extra_metadata
                .as_ref()
                .and_then(|m| m.get("schema"))
                .ok_or_else(|| CoreError::Schema("Latest commit has no logical schema".into()))?;
            let parse = |json: &str| {
                serde_json::from_str::<serde_json::Value>(json)
                    .map_err(|e| CoreError::Schema(format!("Invalid persisted writer schema: {e}")))
            };
            if parse(latest_schema)? != parse(schema)? {
                return Err(CoreError::Unsupported(
                    "COW append requires an unchanged logical schema".into(),
                ));
            }
        }
        Self::from_schema(storage, schema, key, last)
    }

    fn from_schema(
        storage: Arc<Storage>,
        avro: &str,
        record_key: &str,
        last_requested: Option<String>,
    ) -> Result<Self> {
        let parsed: serde_json::Value = serde_json::from_str(avro)
            .map_err(|e| CoreError::Schema(format!("Invalid writer schema: {e}")))?;
        let avro_schema = serde_json::to_string(&parsed)
            .map_err(|e| CoreError::Schema(format!("Cannot serialize writer schema: {e}")))?;
        let schema = Arc::new(avro_json_to_arrow_schema(&avro_schema)?);
        if schema
            .fields()
            .iter()
            .any(|f| f.name().starts_with("_hoodie_"))
        {
            return Err(CoreError::Schema(
                "Writer schema must omit Hudi metadata columns".into(),
            ));
        }
        let key = schema.field_with_name(record_key)?;
        if !matches!(
            key.data_type(),
            DataType::Utf8 | DataType::Int32 | DataType::Int64
        ) {
            return Err(CoreError::Unsupported(format!(
                "Unsupported record key '{record_key}' type {}",
                key.data_type()
            )));
        }
        Ok(Self {
            storage,
            schema,
            avro_schema,
            record_key: record_key.into(),
            last_requested,
            shredding: BTreeMap::new(),
        })
    }

    /// Shred a top-level Variant column using an Arrow logical target type.
    /// Objects use Struct fields and arrays use List; incompatible values remain
    /// in the residual. Shredding is opt-in and does not change the table schema.
    pub fn with_variant_shredding(mut self, column: &str, as_type: DataType) -> Result<Self> {
        self.schema
            .field_with_name(column)?
            .try_extension_type::<VariantType>()?;
        self.shredding.insert(column.into(), as_type);
        Ok(self)
    }

    /// Append one batch as a new file group. An empty batch is a no-op.
    ///
    /// Both timestamps are UTC calendar instants with exactly 17 digits; completion
    /// must not precede request. Input validation and encoding precede reservation.
    /// A failed write leaves its pending instant and any orphan file in place;
    /// readers use the completed marker to determine visibility.
    pub async fn append(
        &mut self,
        batch: RecordBatch,
        requested: &str,
        completed: &str,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        for instant in [requested, completed] {
            if instant.len() != 17
                || !instant.bytes().all(|c| c.is_ascii_digit())
                || chrono::NaiveDateTime::parse_from_str(instant, "%Y%m%d%H%M%S%3f").is_err()
            {
                return Err(CoreError::InvalidValue(format!(
                    "Invalid UTC commit instant '{instant}'"
                )));
            }
        }
        if completed < requested
            || self
                .last_requested
                .as_deref()
                .is_some_and(|last| requested <= last)
        {
            return Err(CoreError::InvalidValue(format!(
                "Commit '{requested}_{completed}' must advance the writer timeline and complete after its request"
            )));
        }
        if batch.num_columns() != self.schema.fields().len()
            || self
                .schema
                .fields()
                .iter()
                .any(|f| batch.schema().index_of(f.name()).is_err())
        {
            return Err(CoreError::Schema(
                "Append batch must contain exactly the table's logical columns".into(),
            ));
        }
        for field in self.schema.fields() {
            validate_write_type(batch.schema().field_with_name(field.name())?, field)?;
        }
        let batch = project_batch_to_schema(&batch, &self.schema)?;
        let key = arrow_cast::cast(
            batch.column_by_name(&self.record_key).ok_or_else(|| {
                CoreError::Schema(format!("Missing record key '{}'", self.record_key))
            })?,
            &DataType::Utf8,
        )?;
        if key.null_count() > 0 || key.as_string::<i32>().iter().flatten().any(str::is_empty) {
            return Err(CoreError::InvalidValue(format!(
                "Record key '{}' must be non-null and nonempty",
                self.record_key
            )));
        }
        let file_id = format!("{requested}-0");
        let file_name = format!("{file_id}_0-0-0_{requested}.parquet");
        let count = batch.num_rows();
        let strings = |s: &str| -> ArrayRef { Arc::new(StringArray::from(vec![s; count])) };
        let mut columns = vec![
            strings(requested),
            Arc::new(StringArray::from_iter_values(
                (0..count).map(|i| format!("{requested}_0_{i}")),
            )) as ArrayRef,
            key,
            strings(""),
            strings(&file_name),
        ];
        let mut fields = MetaField::schema().fields().to_vec();
        for (field, array) in self.schema.fields().iter().zip(batch.columns()) {
            if let Some(as_type) = self.shredding.get(field.name()) {
                let variant = VariantArray::try_new(array)?;
                for row in 0..variant.len() {
                    if !variant.is_null(row) {
                        variant.try_value(row)?.with_full_validation()?;
                    }
                }
                let shredded = shred_variant(&variant, as_type)?;
                fields.push(Arc::new(shredded.field(field.name())));
                columns.push(Arc::new(shredded.into_inner()));
            } else {
                fields.push(field.clone());
                columns.push(array.clone());
            }
        }
        let output = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        let full_avro = crate::schema::prepend_meta_fields_to_avro_schema_str(&self.avro_schema)?;
        let data = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let props = WriterProperties::builder()
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    "parquet.avro.schema".into(),
                    Some(full_avro),
                )]))
                .build();
            let converted = ArrowSchemaConverter::new().convert(output.schema().as_ref())?;
            let parquet_schema = SchemaDescriptor::new(variant_v1(&converted.root_schema_ptr())?);
            let options = ArrowWriterOptions::new()
                .with_properties(props)
                .with_parquet_schema(parquet_schema);
            let mut writer =
                ArrowWriter::try_new_with_options(Vec::new(), output.schema(), options)?;
            writer.write(&output)?;
            Ok(writer.into_inner()?)
        })
        .await
        .map_err(|e| CoreError::FileGroup(format!("Parquet encoding task failed: {e}")))??;
        let stats = HoodieWriteStat {
            file_id: Some(file_id),
            path: Some(file_name.clone()),
            partition_path: Some(String::new()),
            prev_commit: Some("null".into()),
            num_writes: Some(count as i64),
            num_inserts: Some(count as i64),
            num_deletes: Some(0),
            num_update_writes: Some(0),
            total_write_errors: Some(0),
            file_size_in_bytes: Some(data.len() as i64),
            total_write_bytes: Some(data.len() as i64),
            ..Default::default()
        };
        let metadata = HoodieCommitMetadata {
            version: Some(1),
            operation_type: Some("INSERT".into()),
            compacted: Some(false),
            partition_to_write_stats: Some(HashMap::from([(String::new(), vec![stats])])),
            extra_metadata: Some(HashMap::from([("schema".into(), self.avro_schema.clone())])),
            ..Default::default()
        };
        let schema = HoodieCommitMetadata::get_schema();
        let mut metadata_writer = apache_avro::Writer::builder()
            .schema(&schema)
            .writer(Vec::new())
            .marker(*b"hudi-rs-commit01")
            .build();
        metadata_writer.append_ser(metadata)?;
        let commit = metadata_writer.into_inner()?;
        self.put_new(
            &format!(".hoodie/timeline/{requested}.commit.requested"),
            Vec::new(),
        )
        .await?;
        self.last_requested = Some(requested.into());
        self.put_new(
            &format!(".hoodie/timeline/{requested}.inflight"),
            Vec::new(),
        )
        .await?;
        match self
            .put_new(
                ".hoodie_partition_metadata",
                format!("commitTime={requested}\npartitionDepth=0\n").into_bytes(),
            )
            .await
        {
            Ok(())
            | Err(StorageError::ObjectStoreError(object_store::Error::AlreadyExists { .. })) => {}
            Err(e) => return Err(e.into()),
        }
        self.put_new(&file_name, data).await?;
        self.put_new(
            &format!(".hoodie/timeline/{requested}_{completed}.commit"),
            commit,
        )
        .await?;
        Ok(())
    }

    async fn put_new(
        &self,
        relative: &str,
        data: Vec<u8>,
    ) -> std::result::Result<(), StorageError> {
        let url = join_url_segments(&self.storage.base_url, &[relative])?;
        let path = object_store::path::Path::from_url_path(url.path())?;
        self.storage
            .object_store
            .put_opts(
                &path,
                data.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }
}

fn escape_property(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ' '..='~' => escaped.push(ch),
            _ => {
                for unit in ch.encode_utf16(&mut [0; 2]) {
                    escaped.push_str(&format!("\\u{unit:04x}"));
                }
            }
        }
    }
    escaped
}

// Arrow 59 omits specification_version; parquet-java interprets that as zero,
// while Spark's Variant reader accepts version 1. Keep the standard annotation
// and its encoding version explicit without changing physical fields or levels.
fn variant_v1(node: &TypePtr) -> Result<TypePtr> {
    if node.is_primitive() {
        return Ok(node.clone());
    }
    let info = node.get_basic_info();
    let logical = match info.logical_type_ref() {
        Some(LogicalType::Variant(version)) if version.specification_version.is_none() => {
            Some(LogicalType::variant(Some(1)))
        }
        other => other.cloned(),
    };
    let fields = node
        .get_fields()
        .iter()
        .map(variant_v1)
        .collect::<Result<Vec<_>>>()?;
    let mut builder = Type::group_type_builder(info.name())
        .with_converted_type(info.converted_type())
        .with_logical_type(logical)
        .with_fields(fields);
    if info.has_id() {
        builder = builder.with_id(Some(info.id()));
    }
    if info.has_repetition() {
        builder = builder.with_repetition(info.repetition());
    }
    Ok(Arc::new(builder.build()?))
}

// Read-time evolution permits casts. A fixed-schema writer must reject a
// mismatched primitive type rather than narrow it or turn overflow into null.
fn validate_write_type(source: &Field, target: &Field) -> Result<()> {
    if target.has_valid_extension_type::<VariantType>() {
        return VariantType
            .supports_data_type(source.data_type())
            .map_err(|e| CoreError::Schema(format!("Invalid Variant '{}': {e}", target.name())));
    }
    match (source.data_type(), target.data_type()) {
        (DataType::Struct(source), DataType::Struct(target)) if source.len() == target.len() => {
            for field in target {
                let found = source
                    .iter()
                    .find(|f| f.name() == field.name())
                    .ok_or_else(|| {
                        CoreError::Schema(format!("Missing struct field '{}'", field.name()))
                    })?;
                validate_write_type(found, field)?;
            }
            Ok(())
        }
        (DataType::List(source), DataType::List(target))
        | (DataType::Map(source, _), DataType::Map(target, _)) => {
            validate_write_type(source, target)
        }
        (source, target) if source == target => Ok(()),
        _ => Err(CoreError::Schema(format!(
            "Field '{}' requires {}, got {}",
            target.name(),
            target.data_type(),
            source.data_type()
        ))),
    }
}
