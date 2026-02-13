use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{BinaryBuilder, TimestampSecondBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client as S3Client, error::ProvideErrorMetadata, primitives::ByteStream};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    errors::ParquetError,
    file::properties::{WriterProperties, WriterVersion},
};
use thiserror::Error;
use tokio::sync::OnceCell;

use crate::{compat_bincode, types::BlockEvent};

pub const SLOTS_PER_PARTITION: u64 = 1_000;
const WRITE_OPTIMIZED_MAX_ROW_GROUP_ROWS: usize = 1_000;
const ZSTD_COMPRESSION_LEVEL: i32 = 6;
const DEFAULT_ZSTD_THREADS_PER_WRITER: u32 = 1;
const MAX_ZSTD_THREADS_PER_WRITER: u32 = 8;
// Keep batch payloads modest to reduce peak RSS while still writing contiguous parquet batches.
const MAX_BINARY_BYTES_PER_BATCH: usize = 1_024 * 1_024 * 1_024 * 3 / 2;

#[derive(Debug, Error)]
pub enum WriterError {
    #[error("parquet error: {0}")]
    Parquet(#[from] ParquetError),
    #[error("bincode encode error: {0}")]
    Bincode(#[from] compat_bincode::EncodeError),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("S3 put error: {0}")]
    S3Put(String),
    #[error("S3 head error: {0}")]
    S3Head(String),
    #[error("ByteStream error: {0}")]
    ByteStream(String),
}

fn blocks_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("time", DataType::Timestamp(TimeUnit::Second, None), false),
        Field::new("block", DataType::Binary, false),
    ]))
}

struct BlockBatchBuilder {
    slot_builder: UInt64Builder,
    block_time_builder: TimestampSecondBuilder,
    block_builder: BinaryBuilder,
    rows: usize,
    binary_bytes: usize,
}

impl BlockBatchBuilder {
    fn new() -> Self {
        Self {
            slot_builder: UInt64Builder::new(),
            block_time_builder: TimestampSecondBuilder::new(),
            block_builder: BinaryBuilder::new(),
            rows: 0,
            binary_bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.rows == 0
    }

    fn can_fit(&self, additional_binary_bytes: usize) -> bool {
        self.binary_bytes
            .checked_add(additional_binary_bytes)
            .is_some_and(|total| total <= MAX_BINARY_BYTES_PER_BATCH)
    }

    fn append_serialized_block(
        &mut self,
        slot: u64,
        block_time: i64,
        block_data: &[u8],
    ) -> Result<(), WriterError> {
        self.slot_builder.append_value(slot);
        self.block_time_builder.append_value(block_time);
        self.block_builder.append_value(block_data);
        self.rows += 1;
        self.binary_bytes += block_data.len();
        Ok(())
    }

    fn finish(&mut self) -> Result<RecordBatch, WriterError> {
        let batch = RecordBatch::try_new(
            blocks_schema(),
            vec![
                Arc::new(self.slot_builder.finish()),
                Arc::new(self.block_time_builder.finish()),
                Arc::new(self.block_builder.finish()),
            ],
        )?;

        self.rows = 0;
        self.binary_bytes = 0;

        Ok(batch)
    }
}

struct ZstdArrowWriter<W: Write + Send + 'static> {
    writer: ArrowWriter<zstd::Encoder<'static, BufWriter<W>>>,
}

impl<W: Write + Send + 'static> ZstdArrowWriter<W> {
    fn new(
        writer: W,
        schema: SchemaRef,
        zstd_threads_per_writer: u32,
    ) -> Result<Self, ParquetError> {
        let buf_writer = BufWriter::with_capacity(1024 * 1024, writer);
        let mut encoder = zstd::Encoder::new(buf_writer, ZSTD_COMPRESSION_LEVEL)
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        encoder
            .multithread(zstd_threads_per_writer)
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::UNCOMPRESSED)
            .set_max_row_group_size(WRITE_OPTIMIZED_MAX_ROW_GROUP_ROWS)
            .build();

        let writer = ArrowWriter::try_new(encoder, schema, Some(props))?;
        Ok(Self { writer })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        self.writer.write(batch)
    }

    fn close(self) -> Result<(), ParquetError> {
        let encoder = self.writer.into_inner()?;
        encoder
            .finish()
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        Ok(())
    }
}

pub fn slot_to_partition(slot: u64) -> u64 {
    slot / SLOTS_PER_PARTITION
}

pub struct PreparedPartitionUpload {
    pub partition: u64,
    pub rows_written: usize,
    pub s3_key: String,
    pub output_path: PathBuf,
    pub should_cleanup: bool,
}

pub struct PartitionWriter {
    s3_bucket: String,
    s3_prefix: String,
    s3_client: OnceCell<S3Client>,
    temp_dir: PathBuf,
    data_path: Option<PathBuf>,
    zstd_threads_per_writer: u32,
}

impl PartitionWriter {
    pub fn new(
        s3_bucket: String,
        s3_prefix: String,
        temp_dir: PathBuf,
        data_path: Option<PathBuf>,
    ) -> Self {
        let zstd_threads_per_writer = std::env::var("HISTORY_IMPORT_ZSTD_THREADS_PER_WRITER")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .map(|value| value.clamp(1, MAX_ZSTD_THREADS_PER_WRITER))
            .unwrap_or(DEFAULT_ZSTD_THREADS_PER_WRITER);

        Self {
            s3_bucket,
            s3_prefix: s3_prefix.trim_matches('/').to_string(),
            s3_client: OnceCell::new(),
            temp_dir,
            data_path,
            zstd_threads_per_writer,
        }
    }

    pub async fn init_s3(&self) -> &S3Client {
        self.s3_client
            .get_or_init(|| async {
                let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
                S3Client::new(&config)
            })
            .await
    }

    pub async fn partition_exists(&self, partition: u64) -> Result<bool, WriterError> {
        let client = self.init_s3().await;
        let s3_key = self.s3_key_for_partition(partition);

        match client
            .head_object()
            .bucket(&self.s3_bucket)
            .key(&s3_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                if let Some(service_err) = err.as_service_error() {
                    let is_not_found =
                        matches!(service_err.code(), Some("NotFound" | "NoSuchKey" | "404"));
                    if is_not_found {
                        return Ok(false);
                    }
                }
                if err.to_string().contains("404") {
                    return Ok(false);
                }
                Err(WriterError::S3Head(format!("{err:?}")))
            }
        }
    }

    fn s3_key_for_partition(&self, partition: u64) -> String {
        if self.s3_prefix.is_empty() {
            format!("blocks/{partition:010}/block.parquet.zst")
        } else {
            format!(
                "{}/blocks/{partition:010}/block.parquet.zst",
                self.s3_prefix
            )
        }
    }

    fn output_path_for_key(
        &self,
        partition: u64,
        s3_key: &str,
    ) -> Result<(PathBuf, bool), WriterError> {
        if let Some(root) = &self.data_path {
            let local_path = root.join(s3_key);
            if let Some(parent) = local_path.parent() {
                fs::create_dir_all(parent)?;
            }
            Ok((local_path, false))
        } else {
            let unique_suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default();
            let temp_path = self.temp_dir.join(format!(
                "{partition:010}-{}-{unique_suffix}.parquet.zst",
                std::process::id()
            ));
            Ok((temp_path, true))
        }
    }

    pub fn prepare_partition(
        &self,
        partition: u64,
        mut blocks: Vec<BlockEvent>,
    ) -> Result<PreparedPartitionUpload, WriterError> {
        if blocks.is_empty() {
            return Ok(PreparedPartitionUpload {
                partition,
                rows_written: 0,
                s3_key: self.s3_key_for_partition(partition),
                output_path: PathBuf::new(),
                should_cleanup: false,
            });
        }

        // Keep deterministic in-partition ordering before serialization.
        blocks.sort_by_key(|block| block.slot);

        let s3_key = self.s3_key_for_partition(partition);
        let (output_path, should_cleanup) = self.output_path_for_key(partition, &s3_key)?;

        let mut rows_written = 0usize;
        {
            let file = File::create(&output_path)?;
            let mut parquet_writer =
                ZstdArrowWriter::new(file, blocks_schema(), self.zstd_threads_per_writer)?;
            let mut batch_builder = BlockBatchBuilder::new();

            for block in blocks.drain(..) {
                let block_data = compat_bincode::serialize(&block)?;
                if block_data.len() > MAX_BINARY_BYTES_PER_BATCH {
                    return Err(WriterError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "serialized block at slot {} is too large ({} bytes)",
                            block.slot,
                            block_data.len(),
                        ),
                    )));
                }

                if !batch_builder.is_empty() && !batch_builder.can_fit(block_data.len()) {
                    let batch = batch_builder.finish()?;
                    rows_written += batch.num_rows();
                    parquet_writer.write(&batch)?;
                }

                batch_builder.append_serialized_block(block.slot, block.block_time, &block_data)?;
            }

            if !batch_builder.is_empty() {
                let batch = batch_builder.finish()?;
                rows_written += batch.num_rows();
                parquet_writer.write(&batch)?;
            }

            parquet_writer.close()?;
        }

        Ok(PreparedPartitionUpload {
            partition,
            rows_written,
            s3_key,
            output_path,
            should_cleanup,
        })
    }

    pub async fn upload_prepared_partition(
        &self,
        prepared: PreparedPartitionUpload,
    ) -> Result<(), WriterError> {
        if prepared.rows_written == 0 {
            return Ok(());
        }

        let client = self.init_s3().await;
        let body = ByteStream::from_path(&prepared.output_path)
            .await
            .map_err(|e| WriterError::ByteStream(e.to_string()))?;

        client
            .put_object()
            .bucket(&self.s3_bucket)
            .key(&prepared.s3_key)
            .body(body)
            .send()
            .await
            .map_err(|e| WriterError::S3Put(format!("{e:?}")))?;

        log::info!(
            "Uploaded partition {partition} ({} blocks) to s3://{}/{}",
            prepared.rows_written,
            self.s3_bucket,
            prepared.s3_key,
            partition = prepared.partition,
        );

        if prepared.should_cleanup {
            fs::remove_file(&prepared.output_path).ok();
        } else {
            log::info!(
                "Persisted partition {partition} locally at {}",
                prepared.output_path.display(),
                partition = prepared.partition,
            );
        }

        Ok(())
    }
}
