use std::{
    collections::BTreeMap,
    collections::HashSet,
    fs, io,
    ops::Range,
    path::PathBuf,
    sync::OnceLock,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use clickhouse::Client;
use dashmap::DashMap;
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{BlockData, TransactionData, generate_subranges};
use jetstreamer_plugin::{Plugin, PluginFuture};
use solana_reward_info::RewardType;
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    task::JoinSet,
};

use crate::{
    types::{BalanceDiffs, BlockEvent, TransactionTokenBalanceSerde, TxWithMeta},
    writer::{PartitionWriter, PreparedPartitionUpload, SLOTS_PER_PARTITION, slot_to_partition},
};

const MIN_SERIALIZE_QUEUE_CAPACITY: usize = 4;
const MAX_SERIALIZE_QUEUE_CAPACITY: usize = 32;
const SERIALIZE_QUEUE_CAPACITY_PER_THREAD: usize = 1;

const MIN_PREPARED_QUEUE_CAPACITY: usize = 2;
const MAX_PREPARED_QUEUE_CAPACITY: usize = 16;
const PREPARED_QUEUE_CAPACITY_PER_THREAD: usize = 1;

const MAX_IN_FLIGHT_UPLOADS: usize = 4;
const MAX_IN_FLIGHT_SERIALIZERS: usize = 8;
const DEFAULT_MEMORY_BUDGET_FRACTION: f64 = 0.70;
const DEFAULT_MEMORY_RESUME_FRACTION: f64 = 0.85;
const DEFAULT_MEMORY_CHECK_INTERVAL_BLOCKS: u64 = 32;
const MEMORY_BACKPRESSURE_POLL_MS: u64 = 50;
const MIN_MEMORY_BUDGET_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_MEMORY_BUDGET_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MEMORY_BUDGET_MB_ENV: &str = "HISTORY_IMPORT_MEMORY_BUDGET_MB";
const MEMORY_BUDGET_FRACTION_ENV: &str = "HISTORY_IMPORT_MEMORY_BUDGET_FRACTION";
const MEMORY_RESUME_FRACTION_ENV: &str = "HISTORY_IMPORT_MEMORY_RESUME_FRACTION";
const MEMORY_CHECK_INTERVAL_BLOCKS_ENV: &str = "HISTORY_IMPORT_MEMORY_CHECK_INTERVAL_BLOCKS";
const MIN_PARTITION_PROBE_CONCURRENCY: usize = 8;
const MAX_PARTITION_PROBE_CONCURRENCY: usize = 128;
const PARTITION_PROBE_CONCURRENCY_PER_THREAD: usize = 4;

struct BufferState {
    thread_high_watermarks: Vec<u64>,
    partitions: BTreeMap<u64, BTreeMap<u64, BlockEvent>>,
}

struct UploadJob {
    partition: u64,
    blocks: Vec<BlockEvent>,
}

struct UploadQueue {
    sender: mpsc::Sender<UploadJob>,
    error: Arc<Mutex<Option<String>>>,
    serializer_manager: tokio::task::JoinHandle<Result<(), String>>,
    uploader_manager: tokio::task::JoinHandle<Result<(), String>>,
}

pub struct ParquetExportPlugin {
    pending_txs: DashMap<u64, Vec<TxWithMeta>>,
    state: Mutex<BufferState>,
    writer: Arc<PartitionWriter>,
    upload_queue: Mutex<Option<UploadQueue>>,
    serialize_queue_capacity: usize,
    prepared_queue_capacity: usize,
    serialization_concurrency: usize,
    upload_concurrency: usize,
    thread_ranges: Vec<Range<u64>>,
    memory_budget_bytes: u64,
    memory_resume_bytes: u64,
    memory_check_every_n_blocks: u64,
    memory_check_counter: AtomicU64,
    skip_existing_partitions: bool,
    partition_probe_concurrency: usize,
    existing_partitions: OnceLock<HashSet<u64>>,
    start_slot: u64,
    end_slot_exclusive: u64,
}

impl ParquetExportPlugin {
    pub fn new(
        s3_bucket: String,
        s3_prefix: String,
        temp_dir: PathBuf,
        data_path: Option<PathBuf>,
        start_slot: u64,
        end_slot_exclusive: u64,
        threads: usize,
        skip_existing_partitions: bool,
    ) -> Self {
        let thread_count = threads.max(1);
        let slot_range = start_slot..end_slot_exclusive;
        let thread_ranges = generate_subranges(&slot_range, thread_count as u64);
        let thread_high_watermarks = thread_ranges
            .iter()
            .map(|thread_range| thread_range.start.saturating_sub(1))
            .collect();

        let serialize_queue_capacity = Self::derive_queue_capacity(
            thread_count,
            SERIALIZE_QUEUE_CAPACITY_PER_THREAD,
            MIN_SERIALIZE_QUEUE_CAPACITY,
            MAX_SERIALIZE_QUEUE_CAPACITY,
        );
        let prepared_queue_capacity = Self::derive_queue_capacity(
            thread_count,
            PREPARED_QUEUE_CAPACITY_PER_THREAD,
            MIN_PREPARED_QUEUE_CAPACITY,
            MAX_PREPARED_QUEUE_CAPACITY,
        );
        let upload_concurrency = Self::derive_upload_concurrency(thread_count);
        let serialization_concurrency = Self::derive_serialization_concurrency(thread_count);
        let partition_probe_concurrency = Self::derive_queue_capacity(
            thread_count,
            PARTITION_PROBE_CONCURRENCY_PER_THREAD,
            MIN_PARTITION_PROBE_CONCURRENCY,
            MAX_PARTITION_PROBE_CONCURRENCY,
        );
        let memory_budget_bytes = Self::derive_memory_budget_bytes();
        let memory_resume_bytes = Self::derive_memory_resume_bytes(memory_budget_bytes);
        let memory_check_every_n_blocks = Self::derive_memory_check_interval_blocks();

        Self {
            pending_txs: DashMap::new(),
            state: Mutex::new(BufferState {
                thread_high_watermarks,
                partitions: BTreeMap::new(),
            }),
            writer: Arc::new(PartitionWriter::new(
                s3_bucket, s3_prefix, temp_dir, data_path,
            )),
            upload_queue: Mutex::new(None),
            serialize_queue_capacity,
            prepared_queue_capacity,
            serialization_concurrency,
            upload_concurrency,
            thread_ranges,
            memory_budget_bytes,
            memory_resume_bytes,
            memory_check_every_n_blocks,
            memory_check_counter: AtomicU64::new(0),
            skip_existing_partitions,
            partition_probe_concurrency,
            existing_partitions: OnceLock::new(),
            start_slot,
            end_slot_exclusive,
        }
    }

    fn read_meminfo_value_bytes(key: &str) -> Option<u64> {
        let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
        let line = meminfo.lines().find(|line| line.starts_with(key))?;
        let value_kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
        value_kib.checked_mul(1024)
    }

    fn read_process_rss_bytes() -> Option<u64> {
        let status = fs::read_to_string("/proc/self/status").ok()?;
        let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
        let value_kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
        value_kib.checked_mul(1024)
    }

    fn parse_env_f64(name: &str) -> Option<f64> {
        std::env::var(name).ok()?.parse::<f64>().ok()
    }

    fn parse_env_u64(name: &str) -> Option<u64> {
        std::env::var(name).ok()?.parse::<u64>().ok()
    }

    fn derive_memory_budget_bytes() -> u64 {
        if let Some(budget_mb) = Self::parse_env_u64(MEMORY_BUDGET_MB_ENV) {
            return budget_mb
                .saturating_mul(1024 * 1024)
                .max(MIN_MEMORY_BUDGET_BYTES);
        }

        let fraction = Self::parse_env_f64(MEMORY_BUDGET_FRACTION_ENV)
            .unwrap_or(DEFAULT_MEMORY_BUDGET_FRACTION)
            .clamp(0.05, 0.95);

        let available = Self::read_meminfo_value_bytes("MemAvailable:");
        let from_available = available
            .map(|bytes| ((bytes as f64) * fraction) as u64)
            .unwrap_or(DEFAULT_MEMORY_BUDGET_BYTES);

        from_available.max(MIN_MEMORY_BUDGET_BYTES)
    }

    fn derive_memory_resume_bytes(memory_budget_bytes: u64) -> u64 {
        let resume_fraction = Self::parse_env_f64(MEMORY_RESUME_FRACTION_ENV)
            .unwrap_or(DEFAULT_MEMORY_RESUME_FRACTION)
            .clamp(0.50, 0.99);
        ((memory_budget_bytes as f64) * resume_fraction) as u64
    }

    fn derive_memory_check_interval_blocks() -> u64 {
        Self::parse_env_u64(MEMORY_CHECK_INTERVAL_BLOCKS_ENV)
            .unwrap_or(DEFAULT_MEMORY_CHECK_INTERVAL_BLOCKS)
            .clamp(1, 4096)
    }

    fn derive_queue_capacity(
        thread_count: usize,
        per_thread: usize,
        min_capacity: usize,
        max_capacity: usize,
    ) -> usize {
        thread_count
            .saturating_mul(per_thread)
            .clamp(min_capacity, max_capacity)
    }

    fn derive_upload_concurrency(thread_count: usize) -> usize {
        thread_count.clamp(1, MAX_IN_FLIGHT_UPLOADS)
    }

    fn derive_serialization_concurrency(thread_count: usize) -> usize {
        thread_count.clamp(1, MAX_IN_FLIGHT_SERIALIZERS)
    }

    fn requested_partition_bounds(&self) -> Option<(u64, u64)> {
        if self.end_slot_exclusive <= self.start_slot {
            return None;
        }
        let start_partition = slot_to_partition(self.start_slot);
        let end_partition = slot_to_partition(self.end_slot_exclusive.saturating_sub(1));
        Some((start_partition, end_partition))
    }

    fn should_skip_partition(&self, partition: u64) -> bool {
        self.skip_existing_partitions
            && self
                .existing_partitions
                .get()
                .is_some_and(|existing| existing.contains(&partition))
    }

    async fn probe_existing_partitions(
        &self,
    ) -> Result<HashSet<u64>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        if !self.skip_existing_partitions {
            return Ok(HashSet::new());
        }

        let Some((start_partition, end_partition)) = self.requested_partition_bounds() else {
            return Ok(HashSet::new());
        };
        let total_partitions = end_partition
            .saturating_sub(start_partition)
            .saturating_add(1);
        log::info!(
            "Probing S3 for existing partitions in range {}..={} ({} partitions, concurrency {})",
            start_partition,
            end_partition,
            total_partitions,
            self.partition_probe_concurrency,
        );

        let mut existing = HashSet::new();
        let mut in_flight = JoinSet::new();
        let mut next_partition = Some(start_partition);

        while next_partition.is_some() || !in_flight.is_empty() {
            while let Some(partition) = next_partition {
                if in_flight.len() >= self.partition_probe_concurrency {
                    break;
                }
                next_partition = if partition == end_partition {
                    None
                } else {
                    Some(partition + 1)
                };
                let writer = self.writer.clone();
                in_flight.spawn(async move {
                    writer
                        .partition_exists(partition)
                        .await
                        .map(|exists| (partition, exists))
                        .map_err(|err| format!("partition {partition}: {err}"))
                });
            }

            let Some(join_result) = in_flight.join_next().await else {
                break;
            };
            match join_result {
                Ok(Ok((partition, true))) => {
                    existing.insert(partition);
                }
                Ok(Ok((_partition, false))) => {}
                Ok(Err(err)) => return Err(Self::boxed_error(format!("S3 probe failed: {err}"))),
                Err(join_err) => {
                    return Err(Self::boxed_error(format!(
                        "S3 partition probe task panicked: {join_err}"
                    )));
                }
            }
        }

        Ok(existing)
    }

    fn boxed_error(
        message: impl Into<String>,
    ) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        Box::new(io::Error::new(io::ErrorKind::Other, message.into()))
    }

    fn format_mib(bytes: u64) -> u64 {
        bytes / (1024 * 1024)
    }

    fn set_upload_error(error: &Arc<Mutex<Option<String>>>, message: String) {
        let mut guard = error.lock().expect("upload error lock poisoned");
        if guard.is_none() {
            *guard = Some(message);
        }
    }

    fn get_upload_error(error: &Arc<Mutex<Option<String>>>) -> Option<String> {
        error.lock().expect("upload error lock poisoned").clone()
    }

    fn get_pipeline_error(&self) -> Option<String> {
        let upload_queue = self
            .upload_queue
            .lock()
            .expect("upload queue lock poisoned");
        upload_queue
            .as_ref()
            .and_then(|queue| Self::get_upload_error(&queue.error))
    }

    async fn maybe_apply_memory_backpressure(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let check_index = self.memory_check_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if check_index % self.memory_check_every_n_blocks != 0 {
            return Ok(());
        }

        let Some(mut rss_bytes) = Self::read_process_rss_bytes() else {
            return Ok(());
        };
        if rss_bytes <= self.memory_budget_bytes {
            return Ok(());
        }

        log::warn!(
            "RSS {} MiB exceeded memory budget {} MiB; pausing ingestion until below {} MiB",
            Self::format_mib(rss_bytes),
            Self::format_mib(self.memory_budget_bytes),
            Self::format_mib(self.memory_resume_bytes),
        );

        loop {
            if let Some(err) = self.get_pipeline_error() {
                return Err(Self::boxed_error(format!(
                    "upload pipeline unavailable during memory backpressure: {err}"
                )));
            }

            tokio::time::sleep(Duration::from_millis(MEMORY_BACKPRESSURE_POLL_MS)).await;

            let Some(current_rss_bytes) = Self::read_process_rss_bytes() else {
                break;
            };
            rss_bytes = current_rss_bytes;
            if rss_bytes <= self.memory_resume_bytes {
                break;
            }
        }

        log::info!(
            "Memory backpressure released at RSS {} MiB",
            Self::format_mib(rss_bytes)
        );

        Ok(())
    }

    fn handle_join_result<T>(
        join_result: Result<Result<T, String>, tokio::task::JoinError>,
        error: &Arc<Mutex<Option<String>>>,
        worker_label: &str,
    ) -> Result<T, String> {
        match join_result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(err)) => {
                Self::set_upload_error(error, err.clone());
                Err(err)
            }
            Err(join_err) => {
                let message = format!("{worker_label} task panicked: {join_err}");
                Self::set_upload_error(error, message.clone());
                Err(message)
            }
        }
    }

    async fn send_prepared_partition(
        prepared_sender: &mpsc::Sender<PreparedPartitionUpload>,
        prepared: PreparedPartitionUpload,
        error: &Arc<Mutex<Option<String>>>,
    ) -> Result<(), String> {
        if prepared.rows_written == 0 {
            return Ok(());
        }

        let partition = prepared.partition;
        prepared_sender.send(prepared).await.map_err(|_| {
            Self::get_upload_error(error)
                .unwrap_or_else(|| format!("failed to enqueue prepared partition {partition}"))
        })
    }

    async fn run_serialization_manager(
        writer: Arc<PartitionWriter>,
        mut receiver: mpsc::Receiver<UploadJob>,
        prepared_sender: mpsc::Sender<PreparedPartitionUpload>,
        max_in_flight: usize,
        error: Arc<Mutex<Option<String>>>,
    ) -> Result<(), String> {
        let mut in_flight = JoinSet::new();

        loop {
            if in_flight.len() >= max_in_flight {
                if let Some(join_result) = in_flight.join_next().await {
                    let prepared =
                        Self::handle_join_result(join_result, &error, "serialization worker")?;
                    Self::send_prepared_partition(&prepared_sender, prepared, &error).await?;
                }
                continue;
            }

            tokio::select! {
                maybe_job = receiver.recv() => {
                    let Some(job) = maybe_job else {
                        break;
                    };
                    let writer = writer.clone();
                    in_flight.spawn_blocking(move || {
                        let partition = job.partition;
                        writer.prepare_partition(partition, job.blocks).map_err(|e| {
                            format!("partition {partition}: {e}")
                        })
                    });
                }
                Some(join_result) = in_flight.join_next(), if !in_flight.is_empty() => {
                    let prepared = Self::handle_join_result(join_result, &error, "serialization worker")?;
                    Self::send_prepared_partition(&prepared_sender, prepared, &error).await?;
                }
            }
        }

        while let Some(join_result) = in_flight.join_next().await {
            let prepared = Self::handle_join_result(join_result, &error, "serialization worker")?;
            Self::send_prepared_partition(&prepared_sender, prepared, &error).await?;
        }

        drop(prepared_sender);

        Ok(())
    }

    async fn run_upload_manager(
        writer: Arc<PartitionWriter>,
        mut receiver: mpsc::Receiver<PreparedPartitionUpload>,
        max_in_flight: usize,
        error: Arc<Mutex<Option<String>>>,
    ) -> Result<(), String> {
        let mut in_flight = JoinSet::new();

        loop {
            if in_flight.len() >= max_in_flight {
                if let Some(join_result) = in_flight.join_next().await {
                    Self::handle_join_result(join_result, &error, "upload worker")?;
                }
                continue;
            }

            tokio::select! {
                maybe_prepared = receiver.recv() => {
                    let Some(prepared) = maybe_prepared else {
                        break;
                    };
                    let writer = writer.clone();
                    in_flight.spawn(async move {
                        let partition = prepared.partition;
                        writer.upload_prepared_partition(prepared).await.map_err(|e| {
                            format!("partition {partition}: {e}")
                        })
                    });
                }
                Some(join_result) = in_flight.join_next(), if !in_flight.is_empty() => {
                    Self::handle_join_result(join_result, &error, "upload worker")?;
                }
            }
        }

        while let Some(join_result) = in_flight.join_next().await {
            Self::handle_join_result(join_result, &error, "upload worker")?;
        }

        Ok(())
    }

    fn convert_transaction(tx_data: &TransactionData) -> TxWithMeta {
        let meta = &tx_data.transaction_status_meta;
        let error = meta.status.clone().err();
        let logs = meta.log_messages.clone();

        let balance_diffs = Some(BalanceDiffs {
            pre_balances: meta.pre_balances.clone(),
            post_balances: meta.post_balances.clone(),
            pre_token_balances: meta.pre_token_balances.as_ref().map(|balances| {
                balances
                    .iter()
                    .cloned()
                    .map(TransactionTokenBalanceSerde::from)
                    .collect()
            }),
            post_token_balances: meta.post_token_balances.as_ref().map(|balances| {
                balances
                    .iter()
                    .cloned()
                    .map(TransactionTokenBalanceSerde::from)
                    .collect()
            }),
        });

        TxWithMeta {
            transaction: tx_data.transaction.clone(),
            error,
            balance_diffs,
            logs,
        }
    }

    fn convert_keyed_rewards(
        keyed_rewards: &[(solana_address::Address, solana_reward_info::RewardInfo)],
    ) -> solana_transaction_status::Rewards {
        keyed_rewards
            .iter()
            .map(|(address, info)| {
                use solana_transaction_status::RewardType as TSR;

                solana_transaction_status::Reward {
                    pubkey: address.to_string(),
                    lamports: info.lamports,
                    post_balance: info.post_balance,
                    reward_type: Some(match info.reward_type {
                        RewardType::Fee => TSR::Fee,
                        RewardType::Rent => TSR::Rent,
                        RewardType::Staking => TSR::Staking,
                        RewardType::Voting => TSR::Voting,
                    }),
                    commission: info.commission,
                }
            })
            .collect()
    }

    fn update_thread_watermark(&self, state: &mut BufferState, thread_id: usize, slot: u64) {
        if thread_id >= state.thread_high_watermarks.len() {
            state
                .thread_high_watermarks
                .resize(thread_id + 1, self.start_slot.saturating_sub(1));
        }

        let current = &mut state.thread_high_watermarks[thread_id];
        if slot > *current {
            *current = slot;
        }
    }

    fn partition_bounds(partition: u64) -> (u64, u64) {
        let start = partition.saturating_mul(SLOTS_PER_PARTITION);
        let end = start.saturating_add(SLOTS_PER_PARTITION);
        (start, end)
    }

    fn partition_is_ready(&self, state: &BufferState, partition: u64) -> bool {
        let (partition_start, partition_end) = Self::partition_bounds(partition);
        let mut has_overlapping_thread = false;

        for (thread_id, thread_range) in self.thread_ranges.iter().enumerate() {
            let overlap_start = thread_range.start.max(partition_start);
            let overlap_end = thread_range.end.min(partition_end);
            if overlap_start >= overlap_end {
                continue;
            }

            has_overlapping_thread = true;
            let required_high_watermark = overlap_end.saturating_sub(1);
            let current_high_watermark = state
                .thread_high_watermarks
                .get(thread_id)
                .copied()
                .unwrap_or(self.start_slot.saturating_sub(1));
            if current_high_watermark < required_high_watermark {
                return false;
            }
        }

        has_overlapping_thread
    }

    fn drain_next_ready_partition(
        &self,
        state: &mut BufferState,
    ) -> Option<(u64, Vec<BlockEvent>)> {
        let ready_partition = state
            .partitions
            .keys()
            .copied()
            .find(|partition| self.partition_is_ready(state, *partition))?;

        state
            .partitions
            .remove(&ready_partition)
            .map(|slot_map| (ready_partition, slot_map.into_values().collect()))
    }

    async fn queue_partition_upload(
        &self,
        partition: u64,
        blocks: Vec<BlockEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let (sender, upload_error_state) = {
            let upload_queue = self
                .upload_queue
                .lock()
                .expect("upload queue lock poisoned");
            let Some(queue) = upload_queue.as_ref() else {
                return Err(Self::boxed_error("upload queue not initialized"));
            };
            (queue.sender.clone(), queue.error.clone())
        };

        if let Some(err) = Self::get_upload_error(&upload_error_state) {
            return Err(Self::boxed_error(format!(
                "upload pipeline unavailable: {err}"
            )));
        }

        let job = UploadJob { partition, blocks };
        match sender.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(job)) => {
                log::debug!(
                    "Serialization queue full (capacity {}), applying backpressure",
                    self.serialize_queue_capacity,
                );
                sender.send(job).await.map_err(|_| {
                    let reason = Self::get_upload_error(&upload_error_state)
                        .unwrap_or_else(|| "upload queue closed".to_string());
                    Self::boxed_error(format!(
                        "failed to enqueue partition {partition} for serialization: {reason}"
                    ))
                })?;
            }
            Err(TrySendError::Closed(_)) => {
                let reason = Self::get_upload_error(&upload_error_state)
                    .unwrap_or_else(|| "upload queue closed".to_string());
                return Err(Self::boxed_error(format!(
                    "failed to enqueue partition {partition} for serialization: {reason}"
                )));
            }
        }

        if let Some(err) = Self::get_upload_error(&upload_error_state) {
            return Err(Self::boxed_error(format!(
                "upload pipeline unavailable: {err}"
            )));
        }

        Ok(())
    }

    async fn shutdown_upload_queue(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let queue = {
            let mut upload_queue = self
                .upload_queue
                .lock()
                .expect("upload queue lock poisoned");
            upload_queue.take()
        };

        let Some(queue) = queue else {
            return Ok(());
        };

        let UploadQueue {
            sender,
            error,
            serializer_manager,
            uploader_manager,
        } = queue;

        drop(sender);

        let mut first_error: Option<String> = None;

        match serializer_manager.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                first_error = Some(format!("serialization manager failed: {err}"));
            }
            Err(join_err) => {
                first_error = Some(format!("serialization manager task panicked: {join_err}"));
            }
        }

        match uploader_manager.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                if first_error.is_none() {
                    first_error = Some(format!("upload manager failed: {err}"));
                }
            }
            Err(join_err) => {
                if first_error.is_none() {
                    first_error = Some(format!("upload manager task panicked: {join_err}"));
                }
            }
        }

        if first_error.is_none()
            && let Some(err) = Self::get_upload_error(&error)
        {
            first_error = Some(err);
        }

        if let Some(err) = first_error {
            return Err(Self::boxed_error(format!("upload pipeline failed: {err}")));
        }

        Ok(())
    }
}

impl Plugin for ParquetExportPlugin {
    fn name(&self) -> &'static str {
        "Parquet Export"
    }

    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        let slot = transaction.slot;

        async move {
            if slot < self.start_slot || slot >= self.end_slot_exclusive {
                return Ok(());
            }

            if self.should_skip_partition(slot_to_partition(slot)) {
                return Ok(());
            }

            let tx = Self::convert_transaction(transaction);
            self.pending_txs.entry(slot).or_default().push(tx);
            Ok(())
        }
        .boxed()
    }

    fn on_block<'a>(
        &'a self,
        thread_id: usize,
        _db: Option<Arc<Client>>,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        async move {
            let slot = block.slot();

            let mut pending_block_event = match block {
                BlockData::Block {
                    slot,
                    blockhash,
                    parent_slot,
                    parent_blockhash,
                    rewards,
                    block_time,
                    ..
                } => {
                    let partition = slot_to_partition(*slot);
                    if self.should_skip_partition(partition) {
                        self.pending_txs.remove(slot);
                        None
                    } else {
                        let transactions = self
                            .pending_txs
                            .remove(slot)
                            .map(|(_, txs)| txs)
                            .unwrap_or_default();

                        let converted_rewards = Self::convert_keyed_rewards(&rewards.keyed_rewards);

                        Some(BlockEvent {
                            slot: *slot,
                            blockhash: *blockhash,
                            block_time: block_time.unwrap_or(0),
                            parent_slot: *parent_slot,
                            parent_blockhash: *parent_blockhash,
                            rewards: converted_rewards,
                            transactions,
                        })
                    }
                }
                _ => None,
            };

            let mut watermark_updated = false;
            loop {
                let maybe_ready_partition = {
                    let mut state = self.state.lock().expect("partition state lock poisoned");

                    if !watermark_updated {
                        self.update_thread_watermark(&mut state, thread_id, slot);
                        watermark_updated = true;
                    }

                    if let Some(block_event) = pending_block_event.take()
                        && block_event.slot >= self.start_slot
                        && block_event.slot < self.end_slot_exclusive
                    {
                        let partition = slot_to_partition(block_event.slot);
                        let slots = state.partitions.entry(partition).or_default();
                        if slots.insert(block_event.slot, block_event).is_some() {
                            log::warn!(
                                "Replacing duplicate block event for slot {slot} in partition {partition}"
                            );
                        }
                    }

                    self.drain_next_ready_partition(&mut state)
                };

                let Some((partition, blocks)) = maybe_ready_partition else {
                    break;
                };

                self.queue_partition_upload(partition, blocks).await?;
            }

            self.maybe_apply_memory_backpressure().await?;

            Ok(())
        }
        .boxed()
    }

    fn on_load(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            self.writer.init_s3().await;

            if self.skip_existing_partitions {
                let existing = self.probe_existing_partitions().await?;
                let existing_count = existing.len();
                if self.existing_partitions.set(existing).is_err() {
                    return Err(Self::boxed_error(
                        "existing partition cache already initialized",
                    ));
                }
                log::info!(
                    "Will skip {} partition(s) already present on S3",
                    existing_count
                );
            } else if self.existing_partitions.set(HashSet::new()).is_err() {
                return Err(Self::boxed_error(
                    "existing partition cache already initialized",
                ));
            }

            let mut upload_queue = self.upload_queue.lock().expect("upload queue lock poisoned");
            if upload_queue.is_none() {
                let (sender, serialize_receiver) = mpsc::channel(self.serialize_queue_capacity);
                let (prepared_sender, prepared_receiver) =
                    mpsc::channel(self.prepared_queue_capacity);
                let error = Arc::new(Mutex::new(None));
                let serialization_concurrency = self.serialization_concurrency;
                let upload_concurrency = self.upload_concurrency;
                let writer_for_serializer = self.writer.clone();
                let writer_for_uploader = self.writer.clone();
                let serializer_error = error.clone();
                let uploader_error = error.clone();

                let serializer_manager = tokio::spawn(async move {
                    Self::run_serialization_manager(
                        writer_for_serializer,
                        serialize_receiver,
                        prepared_sender,
                        serialization_concurrency,
                        serializer_error,
                    )
                    .await
                });
                let uploader_manager = tokio::spawn(async move {
                    Self::run_upload_manager(
                        writer_for_uploader,
                        prepared_receiver,
                        upload_concurrency,
                        uploader_error,
                    )
                    .await
                });

                *upload_queue = Some(UploadQueue {
                    sender,
                    error,
                    serializer_manager,
                    uploader_manager,
                });
            }

            log::info!(
                "Parquet Export plugin initialized upload pipeline: serialize queue {}, prepared queue {}, serialization concurrency {}, upload concurrency {}, memory budget {} MiB (resume {} MiB, check every {} blocks), skip existing partitions {}",
                self.serialize_queue_capacity,
                self.prepared_queue_capacity,
                self.serialization_concurrency,
                self.upload_concurrency,
                Self::format_mib(self.memory_budget_bytes),
                Self::format_mib(self.memory_resume_bytes),
                self.memory_check_every_n_blocks,
                self.skip_existing_partitions,
            );
            Ok(())
        }
        .boxed()
    }

    fn on_exit(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            let remaining = {
                let mut state = self.state.lock().expect("partition state lock poisoned");
                std::mem::take(&mut state.partitions)
            };

            if !remaining.is_empty() {
                log::info!(
                    "Flushing {} remaining partition(s) on exit",
                    remaining.len()
                );
            }

            for (partition, slot_map) in remaining {
                self.queue_partition_upload(partition, slot_map.into_values().collect())
                    .await?;
            }

            self.shutdown_upload_queue().await?;

            Ok(())
        }
        .boxed()
    }
}
