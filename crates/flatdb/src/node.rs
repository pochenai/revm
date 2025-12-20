use std::sync::Arc;

use reth_chainspec::ChainSpec;
use reth_db::DatabaseEnv;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_ethereum_primitives::EthPrimitives;
use reth_node_types::{NodeTypes, NodeTypesWithDB};
use reth_provider::EthStorage;

pub use reth_provider::test_utils::MockNodeTypesWithDB;
/// Type configuration for a regular Ethereum node.
/// There is dependency issues if `reth-node-ethereum` crate is imported as dependency, so here we define it again.
#[derive(Debug, Default, Clone, Copy)]
pub struct EthereumNode;

impl NodeTypes for EthereumNode {
    type Primitives = EthPrimitives;
    type ChainSpec = ChainSpec;
    type Storage = EthStorage;
    type Payload = EthEngineTypes;
}

impl NodeTypesWithDB for EthereumNode {
    type DB = Arc<DatabaseEnv>;
}
