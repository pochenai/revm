use std::collections::BTreeMap;

use revm::{
    context::{
        self,
        block::{envelope_to_txenv, import_blocks},
        cfg::CfgEnv,
        transaction::AccessList,
        BlockEnv, ContextTr, TxEnv,
    },
    context_interface::block::BlobExcessGasAndPrice,
    database::{bal::BalDatabase, State},
    primitives::{address, hardfork::SpecId, hex::FromHex, Address, HashMap, KECCAK_EMPTY, U256},
    state::{AccountInfo, Bytecode},
    Context, Database, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext,
};

#[test]
fn test_bal() {
    let mut state = BalDatabase::new(State::builder().build()).with_bal_builder();
    state.bal_index = 0;
    let acct1 = AccountInfo {
        balance: U256::MAX,
        /// Account nonce.
        nonce: 0,
        /// Hash of the raw bytes in `code`, or [`KECCAK_EMPTY`].
        code_hash: KECCAK_EMPTY,
        /// Storage id.
        storage_id: None,
        code: Some(Bytecode::default()),
    };
    let addr1 = Address::from_hex("0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97").unwrap();

    let acct2 = AccountInfo {
        balance: U256::ZERO,
        /// Account nonce.
        nonce: 1,
        /// Hash of the raw bytes in `code`, or [`KECCAK_EMPTY`].
        code_hash: KECCAK_EMPTY,
        /// Storage id.
        storage_id: None,
        code: Some(Bytecode::default()),
    };
    let addr2 = Address::from_hex("0xC6093Fd9cc143F9f058938868b2df2daF9A91d28").unwrap();

    let mut genesis_state = BTreeMap::<Address, AccountInfo>::new();
    genesis_state.insert(addr1, acct1);
    genesis_state.insert(addr2, acct2);

    for (address, account) in genesis_state {
        state.insert_account_with_storage(address, account, HashMap::new());
    }

    let block_env = BlockEnv::default();
    // Create EVM context for each transaction to ensure fresh state access
    let evm_context = Context::mainnet()
        .with_block(&block_env)
        .with_db(&mut state);

    let mut evm = evm_context.build_mainnet();
    evm.db_mut().bal_index += 1;

    let tx1 = TxEnv::builder_for_bench()
        .caller(addr1)
        .to(address!("0xc000000000000000000000000000000000000000"))
        .value(U256::ONE)
        .build_fill();
    let exe_result = evm.transact(tx1).ok().unwrap();

    evm.commit(exe_result.state);

    evm.db_mut().bal_index += 1;
    let mut acl = AccessList::default();
    acl.add_address(address!("0x00000000000000000000000000000000000000ff"));
    let tx2 = TxEnv::builder_for_bench()
        .caller(address!("0x00000000000000000000000000000000000000ff"))
        .access_list(acl)
        .to(address!("0xc000000000000000000000000000000000000000"))
        .build_fill();
    let exe_result = evm.transact(tx2).ok().unwrap();

    evm.commit(exe_result.state);

    if let Some(bal) = state.bal_builder.take() {
        println!("{}", serde_json::to_string_pretty(&bal).unwrap());
        // println!("{:?}", bal);
    }
}

fn execute_blocks(blocks: Vec<context::block::RethBlock>) {
    for block in blocks {
        let block_env = BlockEnv {
            number: U256::from(block.number),
            beneficiary: block.beneficiary,
            timestamp: U256::from(block.timestamp),
            gas_limit: block.gas_limit,
            basefee: block.base_fee_per_gas.unwrap(),
            difficulty: block.difficulty,
            prevrandao: Some(block.mix_hash),
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice::new_with_spec(
                block.excess_blob_gas.unwrap(),
                SpecId::PRAGUE,
            )),
        };

        let body = block.into_body();
        for (index, tx) in body.transactions.iter().enumerate() {
            let block_env_clone = block_env.clone();
            let mut state = BalDatabase::new(State::builder().build()).with_bal_builder();
            state.bal_index = index as u64;
            // Create EVM context for each transaction to ensure fresh state access
            let evm_context = Context::mainnet()
                .with_block(block_env_clone)
                .with_db(&mut state);

            let mut evm = evm_context.build_mainnet();
            let txenv = envelope_to_txenv(tx);
            println!(
                "txid {} sender: {:?}, kind:{:?}",
                index, txenv.caller, txenv.tx_type
            );
            let exe_result = evm.transact(txenv);
            print!("exe_result:{:?}", exe_result)
        }
    }
}

#[test]
fn test_exe_blocks() {
    let blocks = import_blocks();
    execute_blocks(blocks);
}
