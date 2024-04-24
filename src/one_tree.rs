use std::sync::Arc;

use btree_olc::{
    btree::btree_leaf_entries,
    buffer_pool::{BufferPool, ThreeTierBufferPool, TwoTierBufferPool},
};
use crossbeam_epoch::Guard;

use crate::{
    config,
    error::QueryError,
    lock_table::{LockTable, ReadGuard, SharedLocked, WriteGuard},
    BufferPoolDB, DBTuple, TableInterface,
};

pub struct GeneralOneTree<V: Clone + PartialEq + DBTuple, Pool: BufferPool + 'static>
where
    [(); btree_leaf_entries::<usize, V>()]:,
{
    lock_table: LockTable,
    tree: btree_olc::btree::Btree<usize, V, Pool>,
}

pub type TwoTierOneTree<V> = GeneralOneTree<V, TwoTierBufferPool<{ config::SHARD_CNT }>>;

impl<V: Clone + PartialEq + DBTuple, Pool: BufferPool + 'static> GeneralOneTree<V, Pool>
where
    [(); btree_leaf_entries::<usize, V>()]:,
{
    fn new_with_pool(pool: Arc<Pool>) -> Self {
        Self {
            lock_table: LockTable::new(),
            tree: btree_olc::btree::Btree::new(pool),
        }
    }
}

pub struct TwoTierOneTreePool {
    pool: Arc<TwoTierBufferPool<{ config::SHARD_CNT }>>,
}

impl BufferPoolDB for TwoTierOneTreePool {
    fn new(local_mem_byte: usize, _remote_mem_byte: usize) -> Self {
        let buffer_pool = TwoTierBufferPool::new(local_mem_byte, douhua::MemType::DRAM, false);
        Self {
            pool: Arc::new(buffer_pool),
        }
    }
}

impl<V> TableInterface<V> for TwoTierOneTree<V>
where
    V: Clone + PartialEq + DBTuple,
    [(); btree_leaf_entries::<usize, V>()]:,
{
    type Pool = TwoTierOneTreePool;

    fn new(pool: &mut Self::Pool) -> Self {
        Self::new_with_pool(pool.pool.clone())
    }

    fn lock(&self, key: &usize) -> Result<ReadGuard, QueryError> {
        self.lock_inner(key)
    }

    fn lock_mut(&self, key: &usize) -> Result<WriteGuard, QueryError> {
        self.lock_mut_inner(key)
    }

    fn insert<'a>(
        &'a self,
        value: V,
        _lock: &WriteGuard,
        guard: &'a Guard,
    ) -> Result<(), QueryError> {
        self.insert_inner(value, guard)
    }

    unsafe fn bench_finished_loading_hint(&self) {
        self.tree.buffer_pool().bench_finished_loading_hint();
    }

    fn update(
        &self,
        key: &usize,
        value: V,
        lock: &WriteGuard,
        guard: &Guard,
    ) -> Result<Option<V>, QueryError> {
        assert_eq!(key, lock.lock_key());
        self.update_inner(key, value, guard)
    }

    fn get<'a>(
        &'a self,
        key: &usize,
        lock: &impl SharedLocked,
        guard: &'a Guard,
    ) -> Result<V, QueryError> {
        assert_eq!(key, lock.lock_key());
        self.get_inner(key, guard)
    }

    fn scan(
        &self,
        low_key: &usize,
        n: usize,
        out_buffer: &mut [(usize, V)],
        guard: &Guard,
    ) -> Result<usize, QueryError> {
        self.scan_inner(low_key, n, out_buffer, guard)
    }

    fn stats(&mut self) -> Option<serde_json::Value> {
        self.stats_inner()
    }
}

pub type ThreeTierOneTree<V> = GeneralOneTree<V, ThreeTierBufferPool<{ config::SHARD_CNT }>>;

pub struct ThreeTierOneTreePool {
    pool: Arc<ThreeTierBufferPool<{ config::SHARD_CNT }>>,
}

impl BufferPoolDB for ThreeTierOneTreePool {
    fn new(local_mem_byte: usize, remote_mem_byte: usize) -> Self {
        let buffer_pool = ThreeTierBufferPool::new(local_mem_byte, remote_mem_byte, true);
        Self {
            pool: Arc::new(buffer_pool),
        }
    }
}

impl<V> TableInterface<V> for ThreeTierOneTree<V>
where
    V: Clone + PartialEq + DBTuple,
    [(); btree_leaf_entries::<usize, V>()]:,
{
    type Pool = ThreeTierOneTreePool;

    fn new(pool: &mut Self::Pool) -> Self {
        Self::new_with_pool(pool.pool.clone())
    }

    fn lock(&self, key: &usize) -> Result<ReadGuard, QueryError> {
        self.lock_inner(key)
    }

    fn lock_mut(&self, key: &usize) -> Result<WriteGuard, QueryError> {
        self.lock_mut_inner(&key)
    }

    fn insert<'a>(
        &'a self,
        value: V,
        _lock: &WriteGuard,
        guard: &'a Guard,
    ) -> Result<(), QueryError> {
        self.insert_inner(value, guard)
    }

    fn get<'a>(
        &'a self,
        key: &usize,
        lock: &impl SharedLocked,
        guard: &'a Guard,
    ) -> Result<V, QueryError> {
        assert_eq!(key, lock.lock_key());
        self.get_inner(key, guard)
    }

    fn update(
        &self,
        key: &usize,
        value: V,
        lock: &WriteGuard,
        guard: &Guard,
    ) -> Result<Option<V>, QueryError> {
        assert_eq!(key, lock.lock_key());
        self.update_inner(key, value, guard)
    }

    fn scan(
        &self,
        low_key: &usize,
        n: usize,
        out_buffer: &mut [(usize, V)],
        guard: &Guard,
    ) -> Result<usize, QueryError> {
        self.scan_inner(low_key, n, out_buffer, guard)
    }

    unsafe fn bench_finished_loading_hint(&self) {
        self.tree.buffer_pool().bench_finished_loading_hint();
    }

    fn stats(&mut self) -> Option<serde_json::Value> {
        self.stats_inner()
    }
}

impl<V, Pool: BufferPool> GeneralOneTree<V, Pool>
where
    V: Clone + PartialEq + DBTuple,
    [(); btree_leaf_entries::<usize, V>()]:,
{
    fn insert_inner<'a>(&'a self, value: V, guard: &'a Guard) -> Result<(), QueryError> {
        let key = value.primary_key();

        self.tree.insert(key, value, guard).unwrap();

        Ok(())
    }

    fn get_inner<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<V, QueryError> {
        self.tree.get(key, guard).ok_or(QueryError::TupleNotFound)
    }

    fn lock_inner(&self, key: &usize) -> Result<ReadGuard, QueryError> {
        self.lock_table.read_lock(key).ok_or(QueryError::Locked)
    }

    fn lock_mut_inner(&self, key: &usize) -> Result<WriteGuard, QueryError> {
        self.lock_table.write_lock(*key).ok_or(QueryError::Locked)
    }

    fn update_inner(&self, key: &usize, value: V, guard: &Guard) -> Result<Option<V>, QueryError> {
        let old = self.tree.compute_if_present(key, |_v| value.clone(), guard);
        match old {
            Some((o, _n)) => Ok(Some(o)),
            None => Ok(None),
        }
    }

    fn scan_inner(
        &self,
        low_key: &usize,
        n: usize,
        out_buffer: &mut [(usize, V)],
        guard: &Guard,
    ) -> Result<usize, QueryError> {
        let high_key = usize::MAX;
        let range = self.tree.range(low_key, &high_key, guard).take(n);
        let mut scanned = 0;
        for i in range {
            out_buffer[scanned] = i;
            scanned += 1;
        }
        Ok(scanned)
    }

    fn stats_inner(&mut self) -> Option<serde_json::Value> {
        let stats = self.tree.stats();
        Some(serde_json::to_value(stats).unwrap())
    }
}
