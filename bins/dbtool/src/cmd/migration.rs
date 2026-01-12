//!
use std::hash::Hash;
use std::sync::Arc;
use std::thread;
use std::{collections::HashMap, path::PathBuf};

use alloy_primitives::{address, Address, StorageKey};
use clap::Parser;
use flatdb::ProviderRW;
use librocksdb::store::{RocksDBError, Store};
use reth_db_api::models::CompactU256;
use reth_db_api::table::{Compress, DupSort, Encode};
use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    table::Table,
    transaction::DbTx,
};

use reth_db_api::{Bytecodes, PlainAccountState, PlainStorageState};

/// `migration` subcommand
#[derive(Parser, Debug, Default)]
pub struct Cmd {
    /// source mdbx path
    #[arg(long)]
    src: String,
    /// target rocksdb path
    #[arg(long)]
    dst: String,
}

#[derive(Debug)]
enum DbType {
    FlatAccount,
    ContractCode,
    FlatStorage,
}

#[cfg(test)]
const COMMIT_BATCH_SIZE: u64 = 5;
#[cfg(test)]
const COMMIT_BATCH_SIZE_CODE: u64 = 5;

#[cfg(not(test))]
const COMMIT_BATCH_SIZE: u64 = 10_000_000;
#[cfg(not(test))]
const COMMIT_BATCH_SIZE_CODE: u64 = 400_000;

impl Cmd {
    ///
    pub fn run<const DEBUG: bool>(&self) {
        let src = flatdb::MainnetProviderRW::new((&self.src).into());
        let src = Arc::new(src);
        let dst = Store::new_rocksdb_backend(&self.dst);
        let dst = Arc::new(dst);

        let mut handles = vec![];

        {
            // import storage
            let src = Arc::clone(&src);
            let dst = Arc::clone(&dst);
            let handle = thread::spawn(|| {
                let start_key = None;
                let res =
                    import_rocksdb_from_mdbx_dup(src, dst, start_key, DbType::FlatStorage, DEBUG);

                match res {
                    Err(e) => panic!("failed to import storage db:{:?}", e),
                    _ => {}
                }
            });
            handles.push(handle);
        }

        {
            // import account
            // let src = Arc::clone(&src);
            // let dst = Arc::clone(&dst);
            // let handle = thread::spawn(|| {
            //     let start_key = None;
            //     let res = import_rocksdb_from_mdbx::<PlainAccountState>(
            //         src,
            //         dst,
            //         start_key,
            //         DbType::FlatAccount,
            //         DEBUG,
            //     );

            //     match res {
            //         Err(e) => panic!("failed to import account db:{:?}", e),
            //         _ => {}
            //     }
            // });
            // handles.push(handle);
        }

        {
            // import code
            // let src = Arc::clone(&src);
            // let dst = Arc::clone(&dst);
            // let handle = thread::spawn(|| {
            //     let start_key = None;
            //     let res = import_rocksdb_from_mdbx::<Bytecodes>(
            //         src,
            //         dst,
            //         start_key,
            //         DbType::ContractCode,
            //         DEBUG,
            //     );

            //     match res {
            //         Err(e) => panic!("failed to import bytecode db:{:?}", e),
            //         _ => {}
            //     }
            // });
            // handles.push(handle);
        }

        for h in handles {
            h.join().expect("thread panicked");
        }
    }
}

fn import_rocksdb_from_mdbx<T: Table>(
    src: Arc<flatdb::MainnetProviderRW>,
    dst: Arc<Store>,
    start_key: Option<T::Key>,
    dbty: DbType,
    debug: bool,
) -> Result<(), RocksDBError>
where
    <T as reth_db_api::table::Table>::Key: Hash,
{
    let commit_batch_size = match dbty {
        DbType::ContractCode => COMMIT_BATCH_SIZE_CODE,
        _ => COMMIT_BATCH_SIZE,
    };

    let mut total_entries: u64 = 0;
    let mut batch_entries: u64 = 0;

    let mut items: HashMap<T::Key, T::Value> = HashMap::new();
    let mut next_start_key = start_key;

    'outer: loop {
        // Create a fresh read-only transaction each round to avoid tx_read timeout
        let provider = src.provider_ro();
        let source_tx = provider.into_tx();

        let mut cursor = source_tx.cursor_read::<T>()?;

        let mut iter = cursor.walk(next_start_key.clone())?;

        // Resume iteration from the last processed key.
        // `walk(Some(key))` is inclusive, so we must skip the first item;
        // otherwise, we would reprocess the same key and end up in an infinite loop.
        if next_start_key.is_some() {
            iter.next();
        }

        let mut advanced = false;

        for kv in iter {
            let (k, v) = kv?;

            next_start_key = Some(k.clone());
            items.insert(k, v);

            total_entries += 1;
            batch_entries += 1;
            advanced = true;

            if batch_entries >= commit_batch_size {
                let batch = std::mem::take(&mut items);

                match dbty {
                    DbType::FlatAccount => dst.set_accounts(batch)?,
                    DbType::ContractCode => dst.set_codes(batch)?,
                    _ => panic!("wrong db type"),
                }

                println!(
                    "[flush] batch_entries={}, total_entries={}, db={:?}",
                    batch_entries,
                    format_with_commas(total_entries),
                    dbty
                );

                batch_entries = 0;

                if debug {
                    break 'outer;
                }

                // Drop tx & cursor, restart from next key
                break;
            }
        }

        // No progress → reached end
        if !advanced {
            break;
        }
    }

    // Final flush
    if !items.is_empty() {
        match dbty {
            DbType::FlatAccount => dst.set_accounts(items)?,
            DbType::ContractCode => dst.set_codes(items)?,
            _ => panic!("wrong db type"),
        }
    }

    println!(
        "total inserted entries: {}, db-{:?} total entries:{:?}",
        format_with_commas(total_entries),
        dbty,
        src.provider_ro().tx_ref().entries::<T>()
    );
    Ok(())
}

fn import_rocksdb_from_mdbx_dup(
    src: Arc<flatdb::MainnetProviderRW>,
    dst: Arc<Store>,
    mut start_key: Option<<PlainStorageState as Table>::Key>,
    dbty: DbType,
    debug: bool,
) -> Result<(), RocksDBError> {
    // Total number of entries imported so far (across all batches).
    let mut total_entries: u64 = 0;

    if debug {
        // a random contract address
        start_key = Some(address!("0x388C818CA8B9251b393131C08a736A67ccB19297"));
    }

    // Each loop iteration corresponds to ONE read transaction lifetime.
    // This avoids long-lived MDBX read transactions which can cause
    // reader timeouts and block GC.
    loop {
        // Create a fresh read-only provider and transaction for this batch to avoid tx_read timeout.
        let provider = src.provider_ro();
        let source_tx = provider.into_tx();
        let mut cursor = source_tx.cursor_dup_read::<PlainStorageState>()?;

        // Number of entries in the current batch.
        let mut batch_entries: u64 = 0;

        // Collected items to be flushed to RocksDB in this batch.
        let mut items = Vec::new();

        // The last primary key seen in this batch.
        // Used to resume iteration in the next read transaction.
        let mut last_seen_key = None;

        // Resume iteration from the last processed key.
        // `cursor.next()` is inclusive, so we must skip the first item (through nex_no_dup);
        // otherwise, we would reprocess the same key and end up in an infinite loop.
        if let Some(key) = start_key {
            cursor.seek(key)?;
        }

        // Iterate over the dup table until the batch is full.
        while let Some((k, v)) = cursor.next_no_dup()? {
            let val: CompactU256 = v.value.into();
            items.push((k, v.key, val));
            batch_entries += 1;
            total_entries += 1;

            // Make sure to traverse all slots for a given address in one go.
            // If a contract account has a very large number of slots, we must not
            // break in the middle; otherwise, the next iteration would restart from
            // the beginning of the same address and potentially cause an infinite loop.
            // By finishing all slots for the current address, the next iteration can
            // safely continue with the next address.
            while let Some((k, v)) = cursor.next_dup()? {
                let val: CompactU256 = v.value.into();
                items.push((k, v.key, val));

                batch_entries += 1;
                total_entries += 1;
            }
            last_seen_key = Some(k);

            // Stop reading once the batch size limit is reached.
            if batch_entries >= COMMIT_BATCH_SIZE {
                break;
            }
        }

        // No more data to read; exit the outer loop.
        if items.is_empty() {
            break;
        }

        // Flush the current batch to RocksDB.
        // At this point, the MDBX read transaction is still alive,
        // but will be dropped immediately after the flush.
        dst.set_storages(items)?;

        println!(
            "[flush] batch_entries={}, total_entries={}, db={:?}",
            batch_entries,
            format_with_commas(total_entries),
            dbty
        );

        // In debug mode, process only a single batch and exit early.
        if debug {
            break;
        }

        // Prepare the resume key for the next batch.
        start_key = last_seen_key;

        // Explicitly drop these to make the transaction lifetime obvious.
        // This ensures MDBX can advance GC and avoids long-lived readers.
        drop(cursor);
        drop(source_tx);
    }

    println!(
        "total inserted slots: {}, db-{:?} total entries:{:?}",
        format_with_commas(total_entries),
        dbty,
        src.provider_ro().tx_ref().entries::<PlainStorageState>()
    );

    Ok(())
}

fn format_with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();

    for (i, ch) in s.chars().rev().enumerate() {
        if i != 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }

    out.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, StorageValue, B256};
    use librocksdb::{
        store::{ACCOUNT_CODES, ACCOUNT_FLATKEYVALUE, STORAGE_FLATKEYVALUE},
        table::StorageTableKey,
    };
    use reth_db_api::models::CompactU256;
    use reth_primitives_traits::{Account, Bytecode};

    use super::*;

    #[test]
    fn test_import_rocksdb_from_mdbx_works() {
        let tempdir = tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
        let path = tempdir.path();
        println!("tmp path:{:?}", path);
        let dst = path.to_str().unwrap().to_owned();

        let cmd = Cmd {
            src: "/root/test_nodes/ethereum/execution/reth_full_bak".into(),
            dst: dst.clone(),
        };
        cmd.run::<true>();

        let dst = Store::new_rocksdb_backend(dst);
        // dst.print_all::<Address, Account>(ACCOUNT_FLATKEYVALUE, &[])
        //     .unwrap();

        // dst.print_all::<B256, Bytecode>(ACCOUNT_CODES, &[]).unwrap();

        dst.print_all::<StorageTableKey, CompactU256>(STORAGE_FLATKEYVALUE, &[])
            .unwrap();
    }
}
