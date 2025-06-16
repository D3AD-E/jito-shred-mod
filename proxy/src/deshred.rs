use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::atomic::Ordering,
};

use crate::forwarder::ShredMetrics;
use base64::{engine::general_purpose, Engine};
use itertools::Itertools;
use jito_protos::shredstream::TraceShred;
use log::{debug, warn};
use prost::Message;
use solana_ledger::shred::{
    merkle::{Shred, ShredCode},
    ReedSolomonCache, Shredder,
};
use solana_sdk::{
    clock::{Slot, MAX_PROCESSING_AGE},
    pubkey::Pubkey,
    transaction::VersionedTransaction,
};
// Define the required accounts once
const required: [Pubkey; 4] = [
    Pubkey::from_str_const("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
    Pubkey::from_str_const("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"),
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
    Pubkey::from_str_const("11111111111111111111111111111111"),
];
/// Returns the number of shreds reconstructed
/// Updates all_shreds with current state, and deshredded_entries with returned values
pub fn reconstruct_shreds<'a, I: Iterator<Item = &'a [u8]>>(
    packet_batch_vec: I,
    all_shreds: &mut HashMap<
        Slot,
        HashMap<u32 /* fec_set_index */, (bool /* completed */, HashSet<ComparableShred>)>,
    >,
    deshredded_entries: &mut Vec<(Slot, Vec<std::string::String>)>,
    rs_cache: &ReedSolomonCache,
    metrics: &ShredMetrics,
) -> usize {
    deshredded_entries.clear();
    let mut slot_fec_index_to_iterate = HashSet::new();
    for data in packet_batch_vec {
        match solana_ledger::shred::Shred::new_from_serialized_shred(data.to_vec())
            .and_then(Shred::try_from)
        {
            Ok(shred) => {
                let slot = shred.common_header().slot;
                let fec_set_index = shred.fec_set_index();
                all_shreds
                    .entry(slot)
                    .or_default()
                    .entry(fec_set_index)
                    .or_default()
                    .1
                    .insert(ComparableShred(shred));
                slot_fec_index_to_iterate.insert((slot, fec_set_index));
            }
            Err(e) => {
                if TraceShred::decode(data).is_ok() {
                    continue;
                }
                warn!("Failed to decode shred. Err: {e:?}");
            }
        }
    }

    let mut recovered_count = 0;
    let mut highest_slot_seen = 0;
    for (slot, fec_set_index) in slot_fec_index_to_iterate {
        highest_slot_seen = highest_slot_seen.max(slot);
        let Some((already_deshredded, shreds)) = all_shreds
            .get_mut(&slot)
            .and_then(|fec_set_indexes| fec_set_indexes.get_mut(&fec_set_index))
        else {
            continue;
        };
        if *already_deshredded {
            debug!("already completed slot {slot}");
            continue;
        }

        let (num_expected_data_shreds, num_data_shreds) = can_recover(shreds);

        // haven't received last data shred, haven't seen any coding shreds, so wait until more arrive
        if num_expected_data_shreds == 0
            || (num_data_shreds < num_expected_data_shreds
                && shreds.len() < num_data_shreds as usize)
        {
            // not enough data shreds, not enough shreds to recover
            continue;
        }

        // try to recover if we have enough coding shreds
        let mut recovered_shreds = Vec::new();
        if num_data_shreds < num_expected_data_shreds
            && shreds.len() as u16 >= num_expected_data_shreds
        {
            let merkle_shreds = shreds
                .iter()
                .sorted_by_key(|s| (u8::MAX - s.shred_type() as u8, s.index()))
                .map(|s| s.0.clone())
                .collect_vec();
            let recovered = match solana_ledger::shred::merkle::recover(merkle_shreds, rs_cache) {
                Ok(r) => r,
                Err(e) => {
                    warn!("Failed to recover shreds for slot: {slot}, fec set: {fec_set_index}. Err: {e}");
                    continue;
                }
            };

            for shred in recovered {
                match shred {
                    Ok(shred) => {
                        recovered_count += 1;
                        recovered_shreds.push(ComparableShred(shred));
                        // can also just insert into hashmap, but kept separate for ease of debug
                    }
                    Err(e) => warn!(
                        "Failed to recover shred for slot {slot}, fec set: {fec_set_index}. Err: {e}"
                    ),
                }
            }
        }

        let sorted_deduped_data_payloads = shreds
            .iter()
            .chain(recovered_shreds.iter())
            .filter_map(|s| Some((s, solana_ledger::shred::layout::get_data(s.payload()).ok()?)))
            .sorted_by_key(|(s, _data)| s.index())
            .collect_vec();

        if (sorted_deduped_data_payloads.len() as u16) < num_expected_data_shreds {
            continue;
        }

        // before deshredding:
        let token_id_bytes = required[1].to_bytes();
        let mut saw_token = false;
        for s in &*shreds {
            if s.payload().windows(32).any(|w| w == token_id_bytes) {
                saw_token = true;
                break;
            }
        }
        if !saw_token {
            continue; // skip expensive deshred
        }

        let deshred_payload = match Shredder::deshred_unchecked(
            sorted_deduped_data_payloads
                .iter()
                .map(|(s, _data)| s.payload()),
        ) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "slot {slot} failed to deshred fec_set_index {fec_set_index}. num_expected_data_shreds: {num_expected_data_shreds}, num_data_shreds: {num_data_shreds}. shred set len: {}, recovered shred set len: {},  Err: {e}.",
                    shreds.len(),
                    recovered_shreds.len(),
                );
                metrics
                    .fec_recovery_error_count
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        let entries = match bincode::deserialize::<Vec<solana_entry::entry::Entry>>(
            &deshred_payload,
        ) {
            Ok(e) => e,
            Err(e) => {
                warn!(
                        "slot {slot} fec_set_index {fec_set_index} failed to deserialize bincode payload of size {}. Err: {e}",
                        deshred_payload.len()
                    );
                metrics
                    .bincode_deserialize_error_count
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        let base64_txs: Vec<String> = entries
            .iter()
            .flat_map(|entry| &entry.transactions)
            .filter(|tx| {
                let keys = tx.message.static_account_keys();
                required.iter().all(|r| keys.contains(r))
            })
            .map(|tx| {
                let tx_bytes = bincode::serialize(tx).unwrap();
                general_purpose::STANDARD.encode(tx_bytes)
            })
            .collect();
        if base64_txs.len() == 0 {
            continue;
        }
        deshredded_entries.push((slot, base64_txs));
        if let Some(fec_set) = all_shreds.get_mut(&slot) {
            // done with this fec set index
            let _ = fec_set
                .get_mut(&fec_set_index)
                .map(|(is_completed, fec_set_shreds)| {
                    *is_completed = true;
                    fec_set_shreds.clear();
                });
        }
    }

    if all_shreds.len() > MAX_PROCESSING_AGE && highest_slot_seen > SLOT_LOOKBACK {
        let threshold = highest_slot_seen - SLOT_LOOKBACK;
        all_shreds.retain(|slot, _fec_set_index| *slot >= threshold);
    }

    if recovered_count > 0 {
        metrics
            .recovered_count
            .fetch_add(recovered_count as u64, Ordering::Relaxed);
    }

    recovered_count
}

const SLOT_LOOKBACK: Slot = 50;

/// check if we can reconstruct (having minimum number of data + coding shreds)
fn can_recover(
    shreds: &HashSet<ComparableShred>,
) -> (
    u16, /* num_expected_data_shreds */
    u16, /* num_data_shreds */
) {
    let mut num_expected_data_shreds = 0;
    let mut data_shred_count = 0;
    for shred in shreds {
        match &shred.0 {
            Shred::ShredCode(s) => {
                num_expected_data_shreds = s.coding_header.num_data_shreds;
            }
            Shred::ShredData(s) => {
                data_shred_count += 1;
                if num_expected_data_shreds == 0 && (s.data_complete() || s.last_in_slot()) {
                    num_expected_data_shreds =
                        (shred.0.index() - shred.0.fec_set_index()) as u16 + 1;
                }
            }
        }
    }
    (num_expected_data_shreds, data_shred_count)
}

/// Issue: datashred equality comparison is wrong due to data size being smaller than the 1203 bytes allocated
#[derive(Clone, Debug, Eq)]
pub struct ComparableShred(Shred);

impl std::ops::Deref for ComparableShred {
    type Target = Shred;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Hash for ComparableShred {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            Shred::ShredCode(s) => {
                s.common_header.hash(state);
                s.coding_header.hash(state);
            }
            Shred::ShredData(s) => {
                s.common_header.hash(state);
                s.data_header.hash(state);
            }
        }
    }
}

impl PartialEq for ComparableShred {
    // Custom comparison to avoid random bytes that are part of payload
    fn eq(&self, other: &Self) -> bool {
        match &self.0 {
            Shred::ShredCode(s1) => match &other.0 {
                Shred::ShredCode(s2) => {
                    let solana_ledger::shred::ShredVariant::MerkleCode {
                        proof_size,
                        chained: _,
                        resigned,
                    } = s1.common_header.shred_variant
                    else {
                        return false;
                    };

                    // see https://github.com/jito-foundation/jito-solana/blob/d6c73374e3b4f863436e4b7d4d1ce5eea01cd262/ledger/src/shred/merkle.rs#L346, and re-add the proof component
                    let comparison_len =
                        <ShredCode as solana_ledger::shred::traits::Shred>::SIZE_OF_PAYLOAD
                            .saturating_sub(
                                usize::from(proof_size)
                                    * solana_ledger::shred::merkle::SIZE_OF_MERKLE_PROOF_ENTRY
                                    + if resigned {
                                        solana_ledger::shred::SIZE_OF_SIGNATURE
                                    } else {
                                        0
                                    },
                            );

                    s1.coding_header == s2.coding_header
                        && s1.common_header == s2.common_header
                        && s1.payload[..comparison_len] == s2.payload[..comparison_len]
                }
                Shred::ShredData(_) => false,
            },
            Shred::ShredData(s1) => match &other.0 {
                Shred::ShredCode(_) => false,
                Shred::ShredData(s2) => {
                    let Ok(s1_data) = solana_ledger::shred::layout::get_data(self.payload()) else {
                        return false;
                    };
                    let Ok(s2_data) = solana_ledger::shred::layout::get_data(other.payload())
                    else {
                        return false;
                    };
                    s1.data_header == s2.data_header
                        && s1.common_header == s2.common_header
                        && s1_data == s2_data
                }
            },
        }
    }
}
