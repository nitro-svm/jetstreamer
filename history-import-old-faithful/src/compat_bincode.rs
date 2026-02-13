//! Bincode helpers matching `nitro_utils::bincode` configuration.
//!
//! Uses bincode 2.x with serde compat, fixed-int encoding, and little-endian
//! byte order — the exact same config as nitro-stream, so serialized bytes are
//! identical.

use bincode_2::config::Config;
use serde::Serialize;

pub type EncodeError = bincode_2::error::EncodeError;

fn config() -> impl Config {
    bincode_2::config::standard()
        .with_fixed_int_encoding()
        .with_little_endian()
}

pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, EncodeError> {
    bincode_2::serde::encode_to_vec(value, config())
}
