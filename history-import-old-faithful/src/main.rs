mod compat_bincode;
mod plugin;
mod types;
mod writer;

use clap::Parser;
use jetstreamer::JetstreamerRunner;
use std::path::PathBuf;

use crate::plugin::ParquetExportPlugin;

#[derive(Parser)]
#[command(about = "Export Old Faithful block data to partitioned parquet files on S3")]
struct Args {
    /// First slot to process (inclusive).
    #[arg(long, env)]
    start_slot: u64,

    /// Last slot to process (exclusive).
    #[arg(long, env)]
    end_slot: u64,

    /// S3 bucket for parquet uploads.
    #[arg(long, env)]
    s3_bucket: String,

    /// S3 key prefix (e.g. "data/v1").
    #[arg(long, env, default_value = "")]
    s3_prefix: String,

    /// Number of firehose ingestion threads.
    #[arg(long, env = "JETSTREAMER_THREADS", default_value = "4")]
    threads: usize,

    /// Optional local root directory where partition files are persisted.
    /// Files are written under this root using the same key layout as S3.
    #[arg(long, env)]
    data_path: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    assert!(
        args.start_slot < args.end_slot,
        "start_slot must be less than end_slot"
    );

    let temp_dir = tempfile::tempdir()?;

    let plugin = ParquetExportPlugin::new(
        args.s3_bucket,
        args.s3_prefix,
        temp_dir.path().to_path_buf(),
        args.data_path,
        args.start_slot,
        args.end_slot,
        args.threads,
    );

    JetstreamerRunner::new()
        .with_log_level("info")
        .with_plugin(Box::new(plugin))
        .with_threads(args.threads)
        .with_slot_range_bounds(args.start_slot, args.end_slot)
        .run()?;

    Ok(())
}
