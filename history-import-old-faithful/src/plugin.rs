use std::{path::PathBuf, sync::Arc};

use clickhouse::Client;
use dashmap::DashMap;
use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{BlockData, TransactionData};
use jetstreamer_plugin::{Plugin, PluginFuture};
use solana_reward_info::RewardType;
use tokio::sync::Mutex;

use crate::{
    types::{BalanceDiffs, BlockEvent, TransactionTokenBalanceSerde, TxWithMeta},
    writer::{PartitionWriter, slot_to_partition},
};

pub struct ParquetExportPlugin {
    pending_txs: DashMap<u64, Vec<TxWithMeta>>,
    writer: Mutex<PartitionWriter>,
}

impl ParquetExportPlugin {
    pub fn new(s3_bucket: String, s3_prefix: String, temp_dir: PathBuf) -> Self {
        Self {
            pending_txs: DashMap::new(),
            writer: Mutex::new(PartitionWriter::new(s3_bucket, s3_prefix, temp_dir)),
        }
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
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        async move {
            let BlockData::Block {
                slot,
                blockhash,
                parent_slot,
                parent_blockhash,
                rewards,
                block_time,
                ..
            } = block
            else {
                return Ok(());
            };

            let transactions = self
                .pending_txs
                .remove(slot)
                .map(|(_, txs)| txs)
                .unwrap_or_default();

            let converted_rewards = Self::convert_keyed_rewards(&rewards.keyed_rewards);

            let block_event = BlockEvent {
                slot: *slot,
                blockhash: *blockhash,
                block_time: block_time.unwrap_or(0),
                parent_slot: *parent_slot,
                parent_blockhash: *parent_blockhash,
                rewards: converted_rewards,
                transactions,
            };

            let mut writer = self.writer.lock().await;

            if let Some(current_partition) = writer.current_partition() {
                let block_partition = slot_to_partition(*slot);
                if block_partition != current_partition {
                    writer.flush_and_upload().await.map_err(|e| {
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                    })?;
                }
            }

            writer.append_block(&block_event).map_err(|e| {
                Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
            })?;

            Ok(())
        }
        .boxed()
    }

    fn on_load(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            let writer = self.writer.lock().await;
            writer.init_s3().await;
            log::info!("Parquet Export plugin initialized S3 client");
            Ok(())
        }
        .boxed()
    }

    fn on_exit(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            let mut writer = self.writer.lock().await;
            if writer.has_data() {
                log::info!("Flushing final partition on exit");
                writer.flush_and_upload().await.map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;
            }
            Ok(())
        }
        .boxed()
    }
}
