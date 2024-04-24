#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

mod alloc;
mod backoff;
pub mod bench;
mod error;
mod index;
mod lock_table;
mod one_tree;
mod three_tree_btree;
mod two_tree_btree;
mod utils;

use std::fmt::Debug;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crossbeam_epoch::Guard;
pub use error::QueryError;
use lock_table::SharedLocked;
use lock_table::{ReadGuard, WriteGuard};
pub use one_tree::{ThreeTierOneTree, ThreeTierOneTreePool, TwoTierOneTree, TwoTierOneTreePool};
pub use three_tree_btree::{ThreeTreeBtree, ThreeTreePool};
pub use two_tree_btree::{TwoTreeLowerBtree, TwoTreeLowerBufferPool};
pub use two_tree_btree::{TwoTreeUpperBlindBtree, TwoTreeUpperBlindBufferPool};
pub use two_tree_btree::{TwoTreeUpperBtree, TwoTreeUpperBufferPool};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod config {
    use std::sync::atomic::AtomicUsize;

    pub(crate) const SHARD_CNT: usize = 1;
    pub(crate) static PROMOTION_RATE: AtomicUsize = AtomicUsize::new(100);
    pub(crate) const DEFAULT_LOW_TIER_CACHE: usize = 2 * 1024 * 1024 * 16; // 32 MB
}

#[cfg(not(test))]
mod config {
    use std::sync::atomic::AtomicUsize;
    pub(crate) const SHARD_CNT: usize = 8;
    pub(crate) static PROMOTION_RATE: AtomicUsize = AtomicUsize::new(5);
    pub(crate) const DEFAULT_LOW_TIER_CACHE: usize = 2 * 1024 * 1024 * 16; // 32 MB
}

pub(crate) static EVICT_CNT_PER_BATCH: AtomicUsize = AtomicUsize::new(1);

pub fn get_evcit_cnt_per_batch() -> usize {
    EVICT_CNT_PER_BATCH.load(Ordering::Relaxed)
}

pub fn set_evict_cnt_per_batch(cnt: usize) {
    EVICT_CNT_PER_BATCH.store(cnt, Ordering::Relaxed)
}

pub fn get_promotion_rate() -> usize {
    config::PROMOTION_RATE.load(Ordering::Relaxed)
}

pub fn set_promotion_rate(prob: usize) {
    assert!(prob <= 100);
    config::PROMOTION_RATE.store(prob, Ordering::Relaxed)
}

pub trait DBTuple: Send + Sync + Debug + Clone + PartialEq {
    fn primary_key(&self) -> usize;
}

pub trait BufferPoolDB: Send + Sync {
    fn new(local_mem_byte: usize, remote_mem_byte: usize) -> Self;
}

pub trait TableInterface<V>: Send + Sync
where
    V: DBTuple,
{
    type Pool: BufferPoolDB;

    fn new(pool: &mut Self::Pool) -> Self;

    fn insert<'a>(
        &'a self,
        value: V,
        lock: &WriteGuard,
        guard: &'a Guard,
    ) -> Result<(), QueryError>;

    fn get<'a>(
        &'a self,
        key: &usize,
        lock: &impl SharedLocked,
        guard: &'a Guard,
    ) -> Result<V, QueryError>;

    fn update(
        &self,
        key: &usize,
        value: V,
        lock: &WriteGuard,
        guard: &Guard,
    ) -> Result<Option<V>, QueryError>;

    fn lock(&self, key: &usize) -> Result<ReadGuard, QueryError>;

    fn lock_mut(&self, key: &usize) -> Result<WriteGuard, QueryError>;

    /// # Safety
    /// benchmark only
    /// this is for benchmarking purposes only, it can be helpful to accerlate the benchmark
    unsafe fn bench_finished_loading_hint(&self);

    /// Write scanned (key, values) to out_buffer. Returns the number of values written.
    fn scan(
        &self,
        start_key: &usize,
        n: usize, // number of keys to scan
        out_buffer: &mut [(usize, V)],
        guard: &Guard,
    ) -> Result<usize, QueryError>;

    fn stats(&mut self) -> Option<serde_json::Value> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpperTid<T> {
    ptr: *const T,
}

unsafe impl<T> Send for UpperTid<T> {}
unsafe impl<T> Sync for UpperTid<T> {}

impl<T> From<usize> for UpperTid<T> {
    fn from(val: usize) -> Self {
        Self {
            ptr: val as *const T,
        }
    }
}

impl<T> From<UpperTid<T>> for usize {
    fn from(val: UpperTid<T>) -> Self {
        val.ptr as usize
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TwoTierTid<T> {
    Local(*const T),
    Remote(*const T),
}

unsafe impl<T> Send for TwoTierTid<T> {}
unsafe impl<T> Sync for TwoTierTid<T> {}

impl<T> TwoTierTid<T> {
    const REMOTE_MASK: usize = 1 << 63;
}

impl<T> From<usize> for TwoTierTid<T> {
    fn from(val: usize) -> Self {
        if val & Self::REMOTE_MASK == Self::REMOTE_MASK {
            Self::Remote((val & !Self::REMOTE_MASK) as *const T)
        } else {
            Self::Local(val as *const T)
        }
    }
}

impl<T> From<TwoTierTid<T>> for usize {
    fn from(val: TwoTierTid<T>) -> Self {
        match val {
            TwoTierTid::Local(ptr) => ptr as usize,
            TwoTierTid::Remote(ptr) => (ptr as usize) | TwoTierTid::<T>::REMOTE_MASK,
        }
    }
}
