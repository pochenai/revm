use alloy_primitives::{Address, BlockNumber};
use revm::{
    context::block_states::PreBlockState,
    database::states::cache::MyError,
    primitives::{HashMap, StorageKey, StorageValue},
    state::bal::Bal,
    DatabaseRef,
};

///
pub trait ProviderRW: Sync {
    ///
    fn set_preblock_state(&self, prestate: &PreBlockState);
    ///
    fn set_storage(&self, addr: Address, storage: HashMap<StorageKey, StorageValue>);
    ///
    fn commit_bal_changes(&self, bal: &Bal, finalized_bn: BlockNumber) -> PreBlockState;
    ///
    fn last_finalized_block_number(&self) -> Option<BlockNumber>;
    /// Create a shared provider for one tx to avoid redudant heap allocation,
    ///  it's almost 50% faster than create a lastest provider for each read.
    fn lastest_provider_ro<'a>(&'a self) -> Box<dyn DatabaseRef<Error = MyError> + 'a>;
}
