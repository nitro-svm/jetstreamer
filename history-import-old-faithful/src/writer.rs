use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};

use arrow::{
    array::{BinaryBuilder, TimestampSecondBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    errors::ParquetError,
    file::properties::{WriterProperties, WriterVersion},
};
use thiserror::Error;
use tokio::sync::OnceCell;

use crate::{compat_bincode, types::BlockEvent};

const SLOTS_PER_PARTITION: u64 = 1000;
const WRITE_OPTIMIZED_MAX_ROW_GROUP_ROWS: usize = 1_000;
const ZSTD_COMPRESSION_LEVEL: i32 = 6;

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
}

impl BlockBatchBuilder {
    fn new() -> Self {
        Self {
            slot_builder: UInt64Builder::new(),
            block_time_builder: TimestampSecondBuilder::new(),
            block_builder: BinaryBuilder::new(),
        }
    }

    fn append_block(&mut self, block: &BlockEvent) -> Result<(), WriterError> {
        let block_data = compat_bincode::serialize(block)?;
        self.slot_builder.append_value(block.slot);
        self.block_time_builder.append_value(block.block_time);
        self.block_builder.append_value(block_data);
        Ok(())
    }

    fn finish(&mut self) -> Result<RecordBatch, WriterError> {
        Ok(RecordBatch::try_new(
            blocks_schema(),
            vec![
                Arc::new(self.slot_builder.finish()),
                Arc::new(self.block_time_builder.finish()),
                Arc::new(self.block_builder.finish()),
            ],
        )?)
    }
}

struct ZstdArrowWriter<W: Write + Send + 'static> {
    writer: ArrowWriter<zstd::Encoder<'static, BufWriter<W>>>,
}

impl<W: Write + Send + 'static> ZstdArrowWriter<W> {
    fn new(writer: W, schema: SchemaRef) -> Result<Self, ParquetError> {
        let buf_writer = BufWriter::with_capacity(1024 * 1024, writer);
        let mut encoder = zstd::Encoder::new(buf_writer, ZSTD_COMPRESSION_LEVEL)
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        encoder
            .multithread(1)
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

pub struct PartitionWriter {
    s3_bucket: String,
    s3_prefix: String,
    s3_client: OnceCell<S3Client>,
    current_partition: Option<u64>,
    builder: BlockBatchBuilder,
    temp_dir: PathBuf,
}

impl PartitionWriter {
    pub fn new(s3_bucket: String, s3_prefix: String, temp_dir: PathBuf) -> Self {
        Self {
            s3_bucket,
            s3_prefix: s3_prefix.trim_matches('/').to_string(),
            s3_client: OnceCell::new(),
            current_partition: None,
            builder: BlockBatchBuilder::new(),
            temp_dir,
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

    pub fn append_block(&mut self, block: &BlockEvent) -> Result<(), WriterError> {
        let partition = slot_to_partition(block.slot);

        if let Some(current) = self.current_partition {
            if partition != current {
                return Err(WriterError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "block slot {} belongs to partition {} but writer is on partition {}",
                        block.slot, partition, current
                    ),
                )));
            }
        } else {
            self.current_partition = Some(partition);
        }

        self.builder.append_block(block)?;
        Ok(())
    }

    pub fn current_partition(&self) -> Option<u64> {
        self.current_partition
    }

    pub fn has_data(&self) -> bool {
        self.current_partition.is_some()
    }

    pub async fn flush_and_upload(&mut self) -> Result<(), WriterError> {
        let Some(partition) = self.current_partition.take() else {
            return Ok(());
        };

        let batch = self.builder.finish()?;
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let temp_path = self.temp_dir.join(format!("{partition:010}.parquet.zst"));
        {
            let file = File::create(&temp_path)?;
            let mut writer = ZstdArrowWriter::new(file, blocks_schema())?;
            writer.write(&batch)?;
            writer.close()?;
        }

        let s3_key = if self.s3_prefix.is_empty() {
            format!("blocks/{partition:010}/block.parquet.zst")
        } else {
            format!(
                "{}/blocks/{partition:010}/block.parquet.zst",
                self.s3_prefix
            )
        };

        let client = self.init_s3().await;
        let body = ByteStream::from_path(&temp_path)
            .await
            .map_err(|e| WriterError::ByteStream(e.to_string()))?;

        client
            .put_object()
            .bucket(&self.s3_bucket)
            .key(&s3_key)
            .body(body)
            .send()
            .await
            .map_err(|e| WriterError::S3Put(e.to_string()))?;

        log::info!(
            "Uploaded partition {partition} ({} blocks) to s3://{}/{}",
            batch.num_rows(),
            self.s3_bucket,
            s3_key,
        );

        std::fs::remove_file(&temp_path).ok();

        Ok(())
    }
}
