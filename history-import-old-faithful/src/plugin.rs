use std::{
    collections::BTreeMap,
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use clickhouse::Client;
use dashmap::DashMap;
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{BlockData, TransactionData};
use jetstreamer_plugin::{Plugin, PluginFuture};
use solana_reward_info::RewardType;
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    task::JoinSet,
};

use crate::{
    types::{BalanceDiffs, BlockEvent, TransactionTokenBalanceSerde, TxWithMeta},
    writer::{PartitionWriter, SLOTS_PER_PARTITION, slot_to_partition},
};

const MIN_UPLOAD_QUEUE_CAPACITY: usize = 4;
const MAX_UPLOAD_QUEUE_CAPACITY: usize = 32;
const UPLOAD_QUEUE_CAPACITY_PER_THREAD: usize = 1;
const MAX_IN_FLIGHT_UPLOADS: usize = 4;

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
    manager: tokio::task::JoinHandle<Result<(), String>>,
}

pub struct ParquetExportPlugin {
    pending_txs: DashMap<u64, Vec<TxWithMeta>>,
    state: Mutex<BufferState>,
    writer: Arc<PartitionWriter>,
    upload_queue: Mutex<Option<UploadQueue>>,
    upload_queue_capacity: usize,
    upload_concurrency: usize,
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
    ) -> Self {
        let initial_watermark = start_slot.saturating_sub(1);
        let thread_count = threads.max(1);
        let upload_queue_capacity = Self::derive_upload_queue_capacity(thread_count)
            .clamp(MIN_UPLOAD_QUEUE_CAPACITY, MAX_UPLOAD_QUEUE_CAPACITY);
        let upload_concurrency = Self::derive_upload_concurrency(thread_count);

        Self {
            pending_txs: DashMap::new(),
            state: Mutex::new(BufferState {
                thread_high_watermarks: vec![initial_watermark; thread_count],
                partitions: BTreeMap::new(),
            }),
            writer: Arc::new(PartitionWriter::new(
                s3_bucket, s3_prefix, temp_dir, data_path,
            )),
            upload_queue: Mutex::new(None),
            upload_queue_capacity,
            upload_concurrency,
            start_slot,
            end_slot_exclusive,
        }
    }

    fn derive_upload_queue_capacity(thread_count: usize) -> usize {
        thread_count.saturating_mul(UPLOAD_QUEUE_CAPACITY_PER_THREAD)
    }

    fn derive_upload_concurrency(thread_count: usize) -> usize {
        thread_count.clamp(1, MAX_IN_FLIGHT_UPLOADS)
    }

    fn boxed_error(
        message: impl Into<String>,
    ) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        Box::new(io::Error::new(io::ErrorKind::Other, message.into()))
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

    fn handle_upload_join_result(
        join_result: Result<Result<(), String>, tokio::task::JoinError>,
        error: &Arc<Mutex<Option<String>>>,
    ) -> Result<(), String> {
        match join_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                Self::set_upload_error(error, err.clone());
                Err(err)
            }
            Err(join_err) => {
                let message = format!("upload worker task panicked: {join_err}");
                Self::set_upload_error(error, message.clone());
                Err(message)
            }
        }
    }

    async fn run_upload_manager(
        writer: Arc<PartitionWriter>,
        mut receiver: mpsc::Receiver<UploadJob>,
        max_in_flight: usize,
        error: Arc<Mutex<Option<String>>>,
    ) -> Result<(), String> {
        let mut in_flight = JoinSet::new();

        loop {
            if in_flight.len() >= max_in_flight {
                if let Some(join_result) = in_flight.join_next().await {
                    Self::handle_upload_join_result(join_result, &error)?;
                }
                continue;
            }

            tokio::select! {
                maybe_job = receiver.recv() => {
                    let Some(job) = maybe_job else {
                        break;
                    };
                    let writer = writer.clone();
                    in_flight.spawn(async move {
                        let partition = job.partition;
                        writer.upload_partition(partition, job.blocks).await.map_err(|e| {
                            format!("partition {partition}: {e}")
                        })
                    });
                }
                Some(join_result) = in_flight.join_next(), if !in_flight.is_empty() => {
                    Self::handle_upload_join_result(join_result, &error)?;
                }
            }
        }

        while let Some(join_result) = in_flight.join_next().await {
            Self::handle_upload_join_result(join_result, &error)?;
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

    fn max_complete_partition(min_seen_slot: u64) -> Option<u64> {
        let partition_window = SLOTS_PER_PARTITION - 1;
        if min_seen_slot < partition_window {
            return None;
        }

        Some((min_seen_slot - partition_window) / SLOTS_PER_PARTITION)
    }

    fn drain_ready_partitions(&self, state: &mut BufferState) -> Vec<(u64, Vec<BlockEvent>)> {
        let Some(min_seen_slot) = state.thread_high_watermarks.iter().copied().min() else {
            return Vec::new();
        };

        let Some(mut max_complete) = Self::max_complete_partition(min_seen_slot) else {
            return Vec::new();
        };

        let last_target_partition = slot_to_partition(self.end_slot_exclusive.saturating_sub(1));
        if max_complete > last_target_partition {
            max_complete = last_target_partition;
        }

        let ready_partition_ids: Vec<u64> = state
            .partitions
            .range(..=max_complete)
            .map(|(partition, _)| *partition)
            .collect();

        let mut drained = Vec::with_capacity(ready_partition_ids.len());
        for partition in ready_partition_ids {
            if let Some(slot_map) = state.partitions.remove(&partition) {
                // BTreeMap guarantees ascending slot order.
                drained.push((partition, slot_map.into_values().collect()));
            }
        }

        drained
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
            return Err(Self::boxed_error(format!("uploader unavailable: {err}")));
        }

        let job = UploadJob { partition, blocks };
        match sender.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(job)) => {
                log::debug!(
                    "Upload queue full (capacity {}), applying backpressure",
                    self.upload_queue_capacity,
                );
                sender.send(job).await.map_err(|_| {
                    let reason = Self::get_upload_error(&upload_error_state)
                        .unwrap_or_else(|| "upload queue closed".to_string());
                    Self::boxed_error(format!(
                        "failed to enqueue partition {partition} for upload: {reason}"
                    ))
                })?;
            }
            Err(TrySendError::Closed(_)) => {
                let reason = Self::get_upload_error(&upload_error_state)
                    .unwrap_or_else(|| "upload queue closed".to_string());
                return Err(Self::boxed_error(format!(
                    "failed to enqueue partition {partition} for upload: {reason}"
                )));
            }
        }

        if let Some(err) = Self::get_upload_error(&upload_error_state) {
            return Err(Self::boxed_error(format!("uploader unavailable: {err}")));
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
            manager,
        } = queue;

        drop(sender);

        match manager.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(Self::boxed_error(format!("upload manager failed: {err}"))),
            Err(join_err) => {
                return Err(Self::boxed_error(format!(
                    "upload manager task panicked: {join_err}"
                )));
            }
        }

        if let Some(err) = Self::get_upload_error(&error) {
            return Err(Self::boxed_error(format!("uploader failed: {err}")));
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
        let tx = Self::convert_transaction(transaction);
        let slot = transaction.slot;

        async move {
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

            let maybe_block_event = match block {
                BlockData::Block {
                    slot,
                    blockhash,
                    parent_slot,
                    parent_blockhash,
                    rewards,
                    block_time,
                    ..
                } => {
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
                _ => None,
            };

            let ready_partitions = {
                let mut state = self.state.lock().expect("partition state lock poisoned");
                self.update_thread_watermark(&mut state, thread_id, slot);

                if let Some(block_event) = maybe_block_event
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

                self.drain_ready_partitions(&mut state)
            };

            for (partition, blocks) in ready_partitions {
                self.queue_partition_upload(partition, blocks).await?;
            }

            Ok(())
        }
        .boxed()
    }

    fn on_load(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            self.writer.init_s3().await;

            let mut upload_queue = self.upload_queue.lock().expect("upload queue lock poisoned");
            if upload_queue.is_none() {
                let (sender, receiver) = mpsc::channel(self.upload_queue_capacity);
                let error = Arc::new(Mutex::new(None));
                let writer = self.writer.clone();
                let upload_concurrency = self.upload_concurrency;
                let error_for_task = error.clone();
                let manager = tokio::spawn(async move {
                    Self::run_upload_manager(writer, receiver, upload_concurrency, error_for_task)
                        .await
                });
                *upload_queue = Some(UploadQueue {
                    sender,
                    error,
                    manager,
                });
            }

            log::info!(
                "Parquet Export plugin initialized S3 client and upload queue (capacity {}, concurrency {})",
                self.upload_queue_capacity,
                self.upload_concurrency,
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
