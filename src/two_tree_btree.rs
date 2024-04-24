use std::{marker::PhantomData, sync::Arc};

use btree_olc::{
    btree::{btree_leaf_entries, Btree},
    buffer_pool::{
        self, BufferPool, InMemory, TwoTierBufferPool, TwoTierMemory, TwoTierMemoryBlind,
    },
};
use crossbeam_epoch::Guard;
use metric::{counter, Counter};
use rand::{thread_rng, Rng};

use crate::{
    config,
    lock_table::{LockTable, ReadGuard, SharedLocked, WriteGuard},
    utils::ReferencedTuple,
    BufferPoolDB, DBTuple, QueryError, TableInterface,
};

pub struct TwoTreeBtree<
    V: Clone + PartialEq + DBTuple,
    UpperPool: BufferPool + 'static,
    LowerPool: BufferPool + 'static,
> where
    [(); btree_leaf_entries::<usize, V>()]:,
    [(); btree_leaf_entries::<usize, ReferencedTuple<V>>()]:,
{
    upper_tree: Btree<usize, ReferencedTuple<V>, UpperPool>,
    lower_tree: Btree<usize, V, LowerPool>,
    lock_table: LockTable,
    pt_v: PhantomData<V>,
}

pub type TwoTreeLowerBtree<V> = TwoTreeBtree<V, InMemory, TwoTierBufferPool<{ config::SHARD_CNT }>>;

pub struct TwoTreeLowerBufferPool {
    upper_buffer: Arc<InMemory>,
    lower_buffer: Arc<TwoTierBufferPool<{ config::SHARD_CNT }>>,
}

impl BufferPoolDB for TwoTreeLowerBufferPool {
    fn new(local_mem_byte: usize, remote_mem_byte: usize) -> Self {
        let upper_buffer =
            buffer_pool::InMemory::new(douhua::MemType::DRAM, local_mem_byte as isize);
        let lower_buffer = TwoTierBufferPool::new(
            remote_mem_byte,
            douhua::MemType::NUMA,
            true, // only delay remote memory
        );
        Self {
            upper_buffer: Arc::new(upper_buffer),
            lower_buffer: Arc::new(lower_buffer),
        }
    }
}

impl<V> TableInterface<V> for TwoTreeLowerBtree<V>
where
    V: Clone + PartialEq + DBTuple,
    [(); btree_leaf_entries::<usize, V>()]:,
    [(); btree_leaf_entries::<usize, ReferencedTuple<V>>()]:,
{
    type Pool = TwoTreeLowerBufferPool;
    fn new(pool: &mut Self::Pool) -> Self {
        let upper_tree = Btree::new(pool.upper_buffer.clone());
        let lower_tree = Btree::new(pool.lower_buffer.clone());
        Self {
            upper_tree,
            lower_tree,
            lock_table: LockTable::new(),
            pt_v: PhantomData,
        }
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
        lock: &WriteGuard,
        guard: &'a Guard,
    ) -> Result<(), QueryError> {
        assert_eq!(lock.lock_key(), &value.primary_key());
        self.insert_inner(value, guard)
    }

    fn get<'a>(
        &'a self,
        key: &usize,
        wl: &impl SharedLocked,
        guard: &'a Guard,
    ) -> Result<V, QueryError> {
        self.get_inner(key, guard, wl)
    }

    fn update(
        &self,
        key: &usize,
        value: V,
        lock: &WriteGuard,
        guard: &Guard,
    ) -> Result<Option<V>, QueryError> {
        assert_eq!(lock.lock_key(), key);
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

    fn stats(&mut self) -> Option<serde_json::Value> {
        Some(self.stats_inner())
    }

    unsafe fn bench_finished_loading_hint(&self) {
        self.lower_tree.buffer_pool().bench_finished_loading_hint();
    }
}

pub type TwoTreeUpperBtree<V> =
    TwoTreeBtree<V, TwoTierMemory<{ config::SHARD_CNT }>, TwoTierBufferPool<{ config::SHARD_CNT }>>;

pub struct TwoTreeUpperBufferPool {
    upper_buffer: Arc<TwoTierMemory<{ config::SHARD_CNT }>>,
    lower_buffer: Arc<TwoTierBufferPool<{ config::SHARD_CNT }>>,
}

impl BufferPoolDB for TwoTreeUpperBufferPool {
    fn new(local_mem_byte: usize, remote_mem_byte: usize) -> Self {
        let upper_buffer = TwoTierMemory::new(local_mem_byte as isize, remote_mem_byte as isize);
        let lower_buffer = TwoTierBufferPool::new(
            config::DEFAULT_LOW_TIER_CACHE * config::SHARD_CNT,
            douhua::MemType::DRAM,
            false,
        );
        Self {
            upper_buffer: Arc::new(upper_buffer),
            lower_buffer: Arc::new(lower_buffer),
        }
    }
}

impl<V> TableInterface<V> for TwoTreeUpperBtree<V>
where
    V: Clone + PartialEq + DBTuple,
    [(); btree_leaf_entries::<usize, V>()]:,
    [(); btree_leaf_entries::<usize, ReferencedTuple<V>>()]:,
{
    type Pool = TwoTreeUpperBufferPool;

    fn lock(&self, key: &usize) -> Result<ReadGuard, QueryError> {
        self.lock_inner(key)
    }

    fn lock_mut(&self, key: &usize) -> Result<WriteGuard, QueryError> {
        self.lock_mut_inner(&key)
    }

    fn new(pool: &mut Self::Pool) -> Self {
        let upper_tree = Btree::new(pool.upper_buffer.clone());
        let lower_tree = Btree::new(pool.lower_buffer.clone());
        Self {
            upper_tree,
            lower_tree,
            lock_table: LockTable::new(),
            pt_v: PhantomData,
        }
    }

    fn insert<'a>(
        &'a self,
        value: V,
        lock: &WriteGuard,
        guard: &'a Guard,
    ) -> Result<(), QueryError> {
        assert_eq!(lock.lock_key(), &value.primary_key());
        self.insert_inner(value, guard)
    }

    fn get<'a>(
        &'a self,
        key: &usize,
        lock: &impl SharedLocked,
        guard: &'a Guard,
    ) -> Result<V, QueryError> {
        self.get_inner(key, guard, lock)
    }

    fn update(
        &self,
        key: &usize,
        value: V,
        lock: &WriteGuard,
        guard: &Guard,
    ) -> Result<Option<V>, QueryError> {
        assert_eq!(lock.lock_key(), key);
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
        self.lower_tree.buffer_pool().bench_finished_loading_hint();
    }

    fn stats(&mut self) -> Option<serde_json::Value> {
        Some(self.stats_inner())
    }
}

pub type TwoTreeUpperBlindBtree<V> =
    TwoTreeBtree<V, TwoTierMemoryBlind, TwoTierBufferPool<{ config::SHARD_CNT }>>;

pub struct TwoTreeUpperBlindBufferPool {
    upper_buffer: Arc<TwoTierMemoryBlind>,
    lower_buffer: Arc<TwoTierBufferPool<{ config::SHARD_CNT }>>,
}

impl BufferPoolDB for TwoTreeUpperBlindBufferPool {
    fn new(local_mem_byte: usize, remote_mem_byte: usize) -> Self {
        let upper_buffer =
            TwoTierMemoryBlind::new(local_mem_byte as isize, remote_mem_byte as isize);
        let lower_buffer = TwoTierBufferPool::new(
            config::DEFAULT_LOW_TIER_CACHE * config::SHARD_CNT,
            douhua::MemType::DRAM,
            false,
        );
        Self {
            upper_buffer: Arc::new(upper_buffer),
            lower_buffer: Arc::new(lower_buffer),
        }
    }
}

impl<V> TableInterface<V> for TwoTreeUpperBlindBtree<V>
where
    V: Clone + PartialEq + DBTuple,
    [(); btree_leaf_entries::<usize, V>()]:,
    [(); btree_leaf_entries::<usize, ReferencedTuple<V>>()]:,
{
    type Pool = TwoTreeUpperBlindBufferPool;

    fn lock(&self, key: &usize) -> Result<ReadGuard, QueryError> {
        self.lock_inner(key)
    }

    fn lock_mut(&self, key: &usize) -> Result<WriteGuard, QueryError> {
        self.lock_mut_inner(&key)
    }

    fn new(pool: &mut Self::Pool) -> Self {
        let upper_tree = Btree::new(pool.upper_buffer.clone());
        let lower_tree = Btree::new(pool.lower_buffer.clone());
        Self {
            upper_tree,
            lower_tree,
            lock_table: LockTable::new(),
            pt_v: PhantomData,
        }
    }

    fn update(
        &self,
        key: &usize,
        value: V,
        lock: &WriteGuard,
        guard: &Guard,
    ) -> Result<Option<V>, QueryError> {
        assert_eq!(lock.lock_key(), key);
        self.update_inner(key, value, guard)
    }

    fn insert<'a>(
        &'a self,
        value: V,
        lock: &WriteGuard,
        guard: &'a Guard,
    ) -> Result<(), QueryError> {
        assert_eq!(lock.lock_key(), &value.primary_key());
        self.insert_inner(value, guard)
    }

    fn get<'a>(
        &'a self,
        key: &usize,
        wl: &impl SharedLocked,
        guard: &'a Guard,
    ) -> Result<V, QueryError> {
        self.get_inner(key, guard, wl)
    }

    unsafe fn bench_finished_loading_hint(&self) {
        self.lower_tree.buffer_pool().bench_finished_loading_hint();
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
        Some(self.stats_inner())
    }
}

impl<V: Clone + PartialEq + DBTuple, UpperPool: BufferPool, LowerPool: BufferPool>
    TwoTreeBtree<V, UpperPool, LowerPool>
where
    [(); btree_leaf_entries::<usize, V>()]:,
    [(); btree_leaf_entries::<usize, ReferencedTuple<V>>()]:,
{
    fn insert_inner<'a>(&'a self, value: V, guard: &'a Guard) -> Result<(), QueryError> {
        let key = value.primary_key();

        match self
            .upper_tree
            .insert(key, ReferencedTuple::new_referenced(value), guard)
        {
            Ok(_old) => {
                counter!(metric::Counter::TwoTreeInsertUpper);
                Ok(())
            }
            Err(e) => {
                counter!(metric::Counter::TwoTreeInsertLower);
                self.lower_tree
                    .insert(key, e.not_inserted.value, guard)
                    .expect("Lower tree can't run out of space.");
                Ok(())
            }
        }
    }

    fn get_inner<'a>(
        &'a self,
        key: &usize,
        guard: &'a Guard,
        lock: &impl SharedLocked,
    ) -> Result<V, QueryError> {
        assert_eq!(lock.lock_key(), key);
        if let Some((_, v)) = self.upper_tree.compute_if_present(
            key,
            |v| {
                let mut v = v.clone();
                v.reference();
                v
            },
            guard,
        ) {
            counter!(metric::Counter::DbLocalHit);
            return Ok(v.value);
        };

        let value = self
            .lower_tree
            .get(key, guard)
            .ok_or(QueryError::TupleNotFound)?;

        counter!(metric::Counter::DbRemoteHit);
        if thread_rng().gen_range(0..100) < crate::get_promotion_rate() {
            self.promote_from_lower(key, value.clone(), guard);
            Ok(value)
        } else {
            Ok(value)
        }
    }

    fn promote_from_lower(&self, key: &usize, value: V, guard: &Guard) {
        match self
            .upper_tree
            .insert(*key, ReferencedTuple::new_referenced(value), guard)
        {
            Ok(_old) => {
                self.lower_tree
                    .remove(key, guard)
                    .expect("Old tree must have the value.");
                counter!(Counter::RemoteToLocalOk);
            }
            Err(e) => {
                counter!(Counter::RemoteToLocalFailed);
                if self.evict_from_upper(key, guard).is_some() {
                    self.promote_from_lower(key, e.not_inserted.value, guard)
                }
            }
        }
    }

    /// Returns the evicted key
    fn evict_from_upper(&self, starting_key: &usize, guard: &Guard) -> Option<usize> {
        let scan_cnt = btree_olc::btree::btree_leaf_entries::<usize, ReferencedTuple<V>>();
        let mut evicted: Option<usize> = None;
        self.upper_tree.compute_n_on_key(
            scan_cnt,
            starting_key,
            |k, v| {
                if evicted.is_none() {
                    if v.referenced {
                        v.clear_reference();
                    } else {
                        evicted = Some(*k);
                    }
                }
            },
            guard,
        );
        let k = evicted?;
        if let Some(_wg) = self.lock_table.write_lock(k) {
            if let Some(v) = self.upper_tree.remove(&k, guard) {
                self.lower_tree
                    .insert(k, v.value, guard)
                    .expect("Lower tree can't run out of space.");
                counter!(Counter::TopEvicted);
            }
            Some(k)
        } else {
            None
        }
    }

    fn stats_inner(&mut self) -> serde_json::Value {
        serde_json::json!({
            "upper_tree": self.upper_tree.stats(),
            "lower_tree": self.lower_tree.stats(),
        })
    }

    fn update_inner(&self, key: &usize, value: V, guard: &Guard) -> Result<Option<V>, QueryError> {
        if let Some((o, _n)) = self.upper_tree.compute_if_present(
            key,
            |_v| ReferencedTuple::new_referenced(value.clone()),
            guard,
        ) {
            counter!(metric::Counter::DbLocalHit);
            return Ok(Some(o.value));
        }

        counter!(metric::Counter::DbRemoteHit);

        let old = self
            .lower_tree
            .compute_if_present(key, |_o| value.clone(), guard);
        match old {
            Some((o, _n)) => {
                if thread_rng().gen_range(0..100) < crate::get_promotion_rate() {
                    self.promote_from_lower(key, value, guard);
                }
                Ok(Some(o))
            }
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
        let mut scanned = 0;
        let mut scan_buffer = Vec::with_capacity(n * 2);
        let range = self.upper_tree.range(low_key, &usize::MAX, guard);
        for i in range.take(n) {
            scan_buffer.push((i.0, i.1.value));
            scanned += 1;
        }

        scanned = 0;
        let range = self.lower_tree.range(low_key, &usize::MAX, guard);
        for i in range.take(n) {
            scan_buffer.push((i.0, i.1));
            scanned += 1;
        }

        scanned = 0;
        scan_buffer.sort_by_key(|k| k.0);
        for i in scan_buffer.into_iter().take(n) {
            out_buffer[scanned] = i;
            scanned += 1;
        }
        Ok(scanned)
    }

    fn lock_inner(&self, key: &usize) -> Result<ReadGuard, QueryError> {
        self.lock_table.read_lock(key).ok_or(QueryError::Locked)
    }

    fn lock_mut_inner(&self, key: &usize) -> Result<WriteGuard, QueryError> {
        self.lock_table.write_lock(*key).ok_or(QueryError::Locked)
    }
}
