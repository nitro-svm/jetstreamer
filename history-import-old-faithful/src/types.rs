//! Types mirrored from nitro-stream's `history-model` and `nitro-stream` crates.
//!
//! These must produce identical bincode output so that parquet files written here
//! can be read by `history_model::deserialize_block_event`.

use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use solana_account_decoder::parse_token::UiTokenAmount;
use solana_hash::Hash;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_error::TransactionError;
use solana_transaction_status::Rewards;

/// Mirrors `history_model::types::BlockEvent`.
#[derive(Serialize, Deserialize, Clone)]
pub struct BlockEvent {
    pub slot: u64,
    pub blockhash: Hash,
    pub block_time: i64,
    pub parent_slot: u64,
    pub parent_blockhash: Hash,
    pub rewards: Rewards,
    pub transactions: Vec<TxWithMeta>,
}

/// Mirrors `history_model::types::TxWithMeta`.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TxWithMeta {
    pub transaction: VersionedTransaction,
    pub error: Option<TransactionError>,
    pub balance_diffs: Option<BalanceDiffs>,
    pub logs: Option<Vec<String>>,
}

/// Mirrors `nitro_stream::BalanceDiffs`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BalanceDiffs {
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub pre_token_balances: Option<Vec<TransactionTokenBalanceSerde>>,
    pub post_token_balances: Option<Vec<TransactionTokenBalanceSerde>>,
}

/// Mirrors `nitro_stream::TransactionTokenBalanceSerde`.
///
/// The `serde_as(DisplayFromStr)` attributes on the address fields are
/// critical — they match the serde representation used by nitro-stream so that
/// bincode output is byte-identical.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionTokenBalanceSerde {
    pub account_index: u8,
    #[serde_as(as = "DisplayFromStr")]
    pub mint: solana_address::Address,
    pub ui_token_amount: UiTokenAmount,
    #[serde_as(as = "DisplayFromStr")]
    pub owner: solana_address::Address,
    #[serde_as(as = "DisplayFromStr")]
    pub program_id: solana_address::Address,
}

impl From<solana_transaction_status::TransactionTokenBalance> for TransactionTokenBalanceSerde {
    fn from(value: solana_transaction_status::TransactionTokenBalance) -> Self {
        Self {
            account_index: value.account_index,
            mint: parse_address_or_default(&value.mint),
            ui_token_amount: value.ui_token_amount,
            owner: parse_address_or_default(&value.owner),
            program_id: parse_address_or_default(&value.program_id),
        }
    }
}

fn parse_address_or_default(s: &str) -> solana_address::Address {
    solana_address::Address::from_str(s).unwrap_or_default()
}
