use std::sync::Arc;

use btree_olc::{
    btree::{btree_leaf_entries, Btree},
    buffer_pool::{InMemory, TwoTierBufferPool},
};
use crossbeam_epoch::Guard;
use douhua::MemType;
use metric::{counter, Counter};
use rand::{thread_rng, Rng};

use crate::{
    config,
    lock_table::{LockTable, ReadGuard, SharedLocked, WriteGuard},
    utils::ReferencedTuple,
    BufferPoolDB, DBTuple, QueryError, TableInterface,
};

pub struct ThreeTreeBtree<V: Clone + PartialEq + DBTuple + 'static>
where
    [(); btree_leaf_entries::<usize, V>()]:,
    [(); btree_leaf_entries::<usize, ReferencedTuple<V>>()]:,
{
    lock_table: LockTable,

    top_tree: Btree<usize, ReferencedTuple<V>, InMemory>,
    middle_tree: Btree<usize, ReferencedTuple<V>, InMemory>,
    lower_tree: btree_olc::btree::Btree<usize, V, TwoTierBufferPool<{ config::SHARD_CNT }>>,
}

pub struct ThreeTreePool {
    top_tree_buffer: Arc<InMemory>,
    middle_tree_buffer: Arc<InMemory>,
    bottom_tree_buffer: Arc<TwoTierBufferPool<{ config::SHARD_CNT }>>,
}

impl BufferPoolDB for ThreeTreePool {
    fn new(local_mem_byte: usize, remote_mem_byte: usize) -> Self {
        let top_tree_buffer = Arc::new(InMemory::new(MemType::DRAM, local_mem_byte as isize));
        let middle_tree_buffer = Arc::new(InMemory::new(MemType::NUMA, remote_mem_byte as isize));
        let bottom_tree_buffer = Arc::new(TwoTierBufferPool::new(
            config::DEFAULT_LOW_TIER_CACHE * config::SHARD_CNT,
            MemType::DRAM,
            false,
        ));
        Self {
            top_tree_buffer,
            middle_tree_buffer,
            bottom_tree_buffer,
        }
    }
}

impl<V> TableInterface<V> for ThreeTreeBtree<V>
where
    V: Clone + PartialEq + DBTuple,
    [(); btree_leaf_entries::<usize, V>()]:,
    [(); btree_leaf_entries::<usize, ReferencedTuple<V>>()]:,
{
    type Pool = ThreeTreePool;

    fn lock(&self, key: &usize) -> Result<ReadGuard, QueryError> {
        self.lock_table.read_lock(key).ok_or(QueryError::Locked)
    }

    fn lock_mut(&self, key: &usize) -> Result<WriteGuard, QueryError> {
        self.lock_table.write_lock(*key).ok_or(QueryError::Locked)
    }

    fn new(pool: &mut Self::Pool) -> Self {
        Self {
            lock_table: LockTable::new(),
            top_tree: Btree::new(pool.top_tree_buffer.clone()),
            middle_tree: Btree::new(pool.middle_tree_buffer.clone()),
            lower_tree: Btree::new(pool.bottom_tree_buffer.clone()),
        }
    }

    fn insert<'a>(
        &'a self,
        value: V,
        lock: &WriteGuard,
        guard: &'a Guard,
    ) -> Result<(), QueryError> {
        assert_eq!(*lock.lock_key(), value.primary_key());
        let key = value.primary_key();

        let not_inserted =
            match self
                .top_tree
                .insert(key, ReferencedTuple::new_referenced(value), guard)
            {
                Ok(_old) => return Ok(()),
                Err(v) => v.not_inserted,
            };

        let not_inserted = match self.middle_tree.insert(key, not_inserted, guard) {
            Ok(_old) => return Ok(()),
            Err(v) => v.not_inserted,
        };

        self.lower_tree
            .insert(key, not_inserted.value, guard)
            .expect("Lower tree can't run out of space!");
        Ok(())
    }

    fn get<'a>(
        &'a self,
        key: &usize,
        rl: &impl SharedLocked,
        guard: &'a Guard,
    ) -> Result<V, QueryError> {
        assert_eq!(rl.lock_key(), key);
        if let Some((_, v)) = self.top_tree.compute_if_present(
            key,
            |v| {
                let mut v = v.clone();
                v.reference();
                v
            },
            guard,
        ) {
            counter!(Counter::DbLocalHit);
            #[cfg(debug_assertions)]
            {
                if v.value.primary_key() != *key {
                    let middle_val = self
                        .middle_tree
                        .get(key, guard)
                        .map(|v| v.value.primary_key());
                    let bottom_val = self.lower_tree.get(key, guard).map(|v| v.primary_key());
                    panic!(
                    "Top tree has wrong value, expected: {}, actual: {}!  we found it on middle tree: {:?}, bottom tree: {:?}",
                    v.value.primary_key(),*key,
                    middle_val, bottom_val
                );
                }
            }

            return Ok(v.value);
        }

        if let Some((_, v)) = self.middle_tree.compute_if_present(
            key,
            |v| {
                let mut v = v.clone();
                v.reference();
                v
            },
            guard,
        ) {
            counter!(Counter::DbRemoteHit);

            // if thread_rng().gen_range(0..100) <= crate::get_promotion_rate() {
            if thread_rng().gen_range(0..100) < 0 {
                // don't promote to top tree
                self.promote_from_middle(key, v.value.clone(), guard);
                return Ok(v.value);
            } else {
                assert_eq!(v.value.primary_key(), *key);
                return Ok(v.value);
            }
        }

        let value = self
            .lower_tree
            .get(key, guard)
            .ok_or(QueryError::TupleNotFound)?;
        counter!(Counter::DbDiskHit);
        if thread_rng().gen_range(0..100) <= crate::get_promotion_rate() {
            self.promote_from_lower(key, value.clone(), guard);
            Ok(value)
        } else {
            assert_eq!(value.primary_key(), *key);
            Ok(value)
        }
    }

    fn stats(&mut self) -> Option<serde_json::Value> {
        Some(serde_json::json!(
            {
                "top_tree": self.top_tree.stats(),
                "middle_tree": self.middle_tree.stats(),
                "lower_tree": self.lower_tree.stats(),
            }
        ))
    }

    fn update(
        &self,
        key: &usize,
        value: V,
        lock: &WriteGuard,
        guard: &Guard,
    ) -> Result<Option<V>, QueryError> {
        assert_eq!(lock.lock_key(), key);
        if let Some((o, _n)) = self.top_tree.compute_if_present(
            key,
            |_v| ReferencedTuple::new_referenced(value.clone()),
            guard,
        ) {
            counter!(metric::Counter::DbLocalHit);
            return Ok(Some(o.value));
        }

        if let Some((o, _n)) = self.middle_tree.compute_if_present(
            key,
            |_v| ReferencedTuple::new_referenced(value.clone()),
            guard,
        ) {
            counter!(Counter::DbRemoteHit);

            // if thread_rng().gen_range(0..100) <= crate::get_promotion_rate() {
            if thread_rng().gen_range(0..100) < 0 {
                self.promote_from_middle(key, value, guard);
                return Ok(Some(o.value));
            } else {
                assert_eq!(o.value.primary_key(), *key);
                return Ok(Some(o.value));
            }
        }

        counter!(Counter::DbDiskHit);

        let old = self
            .lower_tree
            .compute_if_present(key, |_o| value.clone(), guard);
        match old {
            Some((o, _n)) => {
                if thread_rng().gen_range(0..100) <= crate::get_promotion_rate() {
                    self.promote_from_lower(key, value, guard);
                }
                Ok(Some(o))
            }
            None => Ok(None),
        }
    }

    fn scan(
        &self,
        low_key: &usize,
        n: usize,
        out_buffer: &mut [(usize, V)],
        guard: &Guard,
    ) -> Result<usize, QueryError> {
        let range = self.top_tree.range(low_key, &usize::MAX, guard);
        let mut scan_buffer = Vec::with_capacity(n * 3); // scan up to n records in each level of trees
        let mut scanned = 0;
        for i in range.take(n) {
            scan_buffer.push((i.0, i.1.value));
            scanned += 1;
        }

        scanned = 0;
        for i in self.middle_tree.range(low_key, &usize::MAX, guard).take(n) {
            scan_buffer.push((i.0, i.1.value));
            scanned += 1;
        }

        scanned = 0;
        for i in self.lower_tree.range(low_key, &usize::MAX, guard).take(n) {
            scan_buffer.push((i.0, i.1));
            scanned += 1;
        }

        scanned = 0;
        scan_buffer.sort_by_key(|a| a.0);
        for i in scan_buffer.into_iter().take(n) {
            out_buffer[scanned] = (i.0, i.1);
            scanned += 1;
        }
        Ok(scanned)
    }

    unsafe fn bench_finished_loading_hint(&self) {
        use btree_olc::buffer_pool::BufferPool;
        self.lower_tree.buffer_pool().bench_finished_loading_hint();
    }
}

impl<V: Clone + PartialEq + DBTuple> ThreeTreeBtree<V>
where
    [(); btree_leaf_entries::<usize, V>()]:,
    [(); btree_leaf_entries::<usize, ReferencedTuple<V>>()]:,
{
    /// Evict a tuple from the top tree, returns the evicted key if successful
    fn evict_from_top(&self, starting_key: &usize, guard: &Guard) -> Option<usize> {
        let scan_cnt = btree_olc::btree::btree_leaf_entries::<usize, ReferencedTuple<V>>();
        let mut evicted: Option<usize> = None;
        self.top_tree.compute_n_on_key(
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

        if let Some(wg) = self.lock_table.write_lock(k) {
            if let Some(mut to_evict) = self.top_tree.remove(&k, guard) {
                assert_eq!(to_evict.value.primary_key(), k);
                to_evict.reference();

                match self.middle_tree.insert(k, to_evict, guard) {
                    Ok(old) => {
                        assert!(old.is_none());
                    }
                    Err(e) => {
                        _ = self.evict_from_middle(&k, guard);
                        self.lower_tree
                            .insert(k, e.not_inserted.value, guard)
                            .expect("No space left on disk!");
                    }
                }
                std::mem::drop(wg);
                Some(k)
            } else {
                // Some other threads delete this value before us.
                None
            }
        } else {
            None
        }
    }

    /// Evict a tuple from the middle tree, returns the evicted key if successful
    fn evict_from_middle(&self, starting_key: &usize, guard: &Guard) -> Option<usize> {
        let scan_cnt = btree_olc::btree::btree_leaf_entries::<usize, ReferencedTuple<V>>();
        let mut evicted: Option<usize> = None;
        // let mut evicted = Vec::with_capacity(EVICT_CNT_PER_BATCH.load(Ordering::Relaxed));
        self.middle_tree.compute_n_on_key(
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
        counter!(Counter::MiddleEvicted);
        if let Some(_wg) = self.lock_table.write_lock(k) {
            if let Some(v) = self.middle_tree.remove(&k, guard) {
                // We have to write to lower tree as we don't know whether the value is in the lower tree.
                self.lower_tree
                    .insert(k, v.value, guard)
                    .expect("Lower tree can't run out of space.");
                Some(k)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn promote_from_middle(&self, key: &usize, value: V, guard: &Guard) {
        match self
            .top_tree
            .insert(*key, ReferencedTuple::new_referenced(value), guard)
        {
            Ok(_old) => {
                self.middle_tree.remove(key, guard);
                counter!(Counter::RemoteToLocalOk);
            }
            Err(_e) => {
                counter!(Counter::RemoteToLocalFailed);
                self.evict_from_top(key, guard);
            }
        }
    }

    fn promote_from_lower(&self, key: &usize, value: V, guard: &Guard) {
        match self
            .top_tree
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
                if self.evict_from_top(key, guard).is_none() {
                    self.promote_from_lower(key, e.not_inserted.value, guard)
                };
            }
        }
    }
}
