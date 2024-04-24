pub(crate) mod basic;
mod inner;
mod iter;
pub(crate) mod leaf;
pub(crate) mod lock;
mod stats;

#[cfg(test)]
mod tests;

use crate::{
    buffer_pool::in_memory::InMemory,
    buffer_pool::BufferPool,
    pid::Pid,
    utils::{Backoff, TreeError},
    InsertOutOfSpaceError, PAGE_SIZE,
};
use basic::BaseNode;
use crossbeam_epoch::Guard;
pub(crate) use inner::BtreeInner;
use iter::BtreeIter;
use leaf::BtreeLeaf;
use lock::ReadGuard;
use metric::counter;
use std::{fmt::Debug, marker::PhantomData, sync::Arc};

pub use {inner::btree_inner_entries, leaf::btree_leaf_entries};

#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicUsize, Ordering};

use self::lock::WriteGuard;

#[repr(u8)]
#[derive(PartialEq, Eq, Debug, Clone)]
pub enum PageType {
    Inner,
    Leaf,
}

pub(crate) const NODE_LAYOUT: std::alloc::Layout = unsafe {
    assert!(PAGE_SIZE.is_power_of_two());
    std::alloc::Layout::from_size_align_unchecked(PAGE_SIZE, PAGE_SIZE)
};

pub struct Btree<K: PartialOrd + Clone + Debug, V: Clone + PartialEq, M: BufferPool = InMemory>
where
    [(); btree_leaf_entries::<K, V>()]:,
    [(); btree_inner_entries::<K>()]:,
{
    root: AtomicUsize,
    mem_src: Arc<M>,

    pt_key: PhantomData<K>,
    pt_value: PhantomData<V>,
}

impl<K: PartialOrd + Clone + Debug, V: Clone + PartialEq> Default for Btree<K, V>
where
    [(); btree_leaf_entries::<K, V>()]:,
    [(); btree_inner_entries::<K>()]:,
{
    fn default() -> Self {
        Self::new(Arc::new(InMemory::default()))
    }
}

impl<K: PartialOrd + Clone + Debug, V: Clone + PartialEq, M: BufferPool> Drop for Btree<K, V, M>
where
    [(); btree_leaf_entries::<K, V>()]:,
    [(); btree_inner_entries::<K>()]:,
{
    fn drop(&mut self) {
        let mut all_nodes = Vec::new();
        all_nodes.push(Pid::from(self.root.load(Ordering::Relaxed)));

        while !all_nodes.is_empty() {
            let backoff = Backoff::new();
            let pid = all_nodes.pop().unwrap();
            'retry_pid: loop {
                let node = self.resolve_pid(pid.clone());
                let mut node = if let Ok(v) = node {
                    if let Ok(v) = v.upgrade() {
                        v
                    } else {
                        backoff.snooze();
                        continue 'retry_pid;
                    }
                } else {
                    // why we might have a contention here? because the buffer pool may happened to evict this page.
                    backoff.snooze();
                    continue 'retry_pid;
                };

                if node.as_ref().page_type == PageType::Inner {
                    let inner: &BtreeInner<K> = node.as_mut().as_inner();
                    for i in 0..(inner.base.count + 1) {
                        let child = inner.children[i as usize].clone();
                        assert!(!pid.is_null());
                        all_nodes.push(child);
                    }
                    self.mem_src.free_page(pid, node);
                } else {
                    let leaf: &mut BtreeLeaf<K, V> = node.as_mut().as_leaf_mut();

                    for i in 0..leaf.base.count {
                        let child = unsafe { std::ptr::read(leaf.values.as_ptr().add(i as usize)) };
                        std::mem::drop(child);
                    }
                    self.mem_src.free_page(pid, node);
                }
                break 'retry_pid;
            }
        }
    }
}

impl<K: PartialOrd + Clone + Debug, V: Clone + PartialEq, M: BufferPool> Btree<K, V, M>
where
    [(); btree_leaf_entries::<K, V>()]:,
    [(); btree_inner_entries::<K>()]:,
{
    pub fn new(mem: Arc<M>) -> Self {
        let root = mem
            .reserve_page(PageType::Leaf)
            .expect("failed to allocate root page");
        let pid = Pid::from(root.as_ref().pid);
        std::mem::drop(root);
        assert!(std::mem::size_of::<BtreeLeaf<K, V>>() <= PAGE_SIZE);
        assert!(std::mem::size_of::<BtreeInner<K>>() <= PAGE_SIZE);
        Self {
            root: AtomicUsize::new(pid.into()),
            pt_key: PhantomData,
            mem_src: mem,
            pt_value: PhantomData,
        }
    }

    /// The interface is deliberately designed such that the caller must clone a pid instead of using the snapshot.
    fn resolve_pid(&self, pid: Pid) -> Result<ReadGuard, TreeError> {
        let backoff = Backoff::new();
        loop {
            let rv = self.mem_src.resolve_pid(pid.clone());

            if unsafe { &*rv }.pid == pid.clone().into() {
                let rl = unsafe { &*rv }.read_lock()?;
                return Ok(rl);
            } else {
                backoff.spin();
            }
        }
    }

    fn get_root(&self) -> Result<ReadGuard, TreeError> {
        let pid = Pid::from(self.root.load(Ordering::Acquire));
        self.resolve_pid(pid)
    }

    fn get_root_pid(&self) -> Pid {
        Pid::from(self.root.load(Ordering::Acquire))
    }

    fn install_root(&self, key: K, left_child: Pid, right_child: Pid, mut new_node: WriteGuard) {
        let inner_mut = new_node.as_mut().as_inner_mut();
        inner_mut.base.count = 1;
        inner_mut.keys[0] = key;
        inner_mut.children[0] = left_child;
        inner_mut.children[1] = right_child;
        self.root
            .store(Pid::from(new_node.as_ref().pid).into(), Ordering::Release);
    }

    pub fn insert(
        &self,
        key: K,
        value: V,
        _guard: &crossbeam_epoch::Guard,
    ) -> Result<Option<V>, InsertOutOfSpaceError<V>> {
        let backoff = Backoff::new();

        'need_restart: loop {
            let (node, parent) = match self.traverse_to_leaf(&key, true) {
                Ok(v) => v,
                Err(e) => match e {
                    TreeError::OutOfSpace(_e) => return Err(InsertOutOfSpaceError::new(value)),
                    _ => {
                        backoff.spin();
                        continue 'need_restart;
                    }
                },
            };

            let leaf = node.as_ref().as_leaf::<K, V>();

            if leaf.is_full() {
                counter!(metric::Counter::BtreeLeafSplit);
                let w_p = if let Some(p) = parent {
                    if let Ok(w_p) = p.0.upgrade() {
                        Some(w_p)
                    } else {
                        backoff.spin();
                        continue 'need_restart;
                    }
                } else {
                    None
                };

                let mut w_n = if let Ok(v) = node.upgrade() {
                    v
                } else {
                    backoff.spin();
                    continue 'need_restart;
                };

                let leaf_next = &w_n.as_mut().as_leaf::<K, V>().next;
                let mut leaf_next = if leaf_next.is_null() {
                    None
                } else if let Ok(v) = self.resolve_pid(leaf_next.clone()) {
                    if let Ok(v) = v.upgrade() {
                        Some(v)
                    } else {
                        backoff.spin();
                        continue 'need_restart;
                    }
                } else {
                    backoff.spin();
                    continue 'need_restart;
                };
                if w_p.is_none() && (w_n.as_ref().pid() != self.get_root_pid()) {
                    backoff.spin();
                    continue 'need_restart;
                }

                match w_p {
                    Some(mut p) => {
                        let (new_leaf, sep) = w_n.as_mut().as_leaf_mut::<K, V>().split(
                            &mut leaf_next,
                            self.mem_src
                                .reserve_page(PageType::Leaf)
                                .map_err(|_e| InsertOutOfSpaceError::new(value.clone()))?,
                        );

                        p.as_mut()
                            .as_inner_mut()
                            .insert(sep, new_leaf.as_ref().pid());
                    }
                    None => {
                        let new_node = self
                            .mem_src
                            .reserve_page(PageType::Leaf)
                            .map_err(|_e| InsertOutOfSpaceError::new(value.clone()))?;
                        let new_root = self.mem_src.reserve_page(PageType::Inner);
                        let new_root = match new_root {
                            Ok(v) => v,
                            Err(_e) => {
                                self.mem_src.free_page(new_node.as_ref().pid(), new_node);
                                return Err(InsertOutOfSpaceError::new(value));
                            }
                        };
                        let w_n_pid = w_n.as_ref().pid();
                        let (new_leaf, sep) = w_n
                            .as_mut()
                            .as_leaf_mut::<K, V>()
                            .split(&mut leaf_next, new_node);

                        self.install_root(sep, w_n_pid, new_leaf.as_ref().pid(), new_root);
                    }
                }
                counter!(metric::Counter::BtreeLeafSplitSuccess);
                continue 'need_restart;
            } else {
                let mut w_n = if let Ok(v) = node.upgrade() {
                    v
                } else {
                    backoff.spin();
                    continue 'need_restart;
                };

                if let Some(p) = parent {
                    if p.0.check_version().is_err() {
                        backoff.spin();
                        continue 'need_restart;
                    }
                }

                let old = w_n.as_mut().as_leaf_mut().insert(key, value);
                return Ok(old);
            }
        }
    }

    pub fn buffer_pool(&self) -> &M {
        &self.mem_src
    }

    fn traverse_to_leaf(
        &self,
        key: &K,
        split_inner: bool,
    ) -> Result<(ReadGuard, Option<(ReadGuard, usize)>), TreeError> {
        let mut node = self.get_root()?;
        if node.as_ref().pid() != self.get_root_pid() {
            return Err(TreeError::NeedRestart);
        }

        let mut parent: Option<(ReadGuard, usize)> = None;

        while node.as_ref().page_type == PageType::Inner {
            let inner: &BtreeInner<K> = node.as_ref().as_inner();

            debug_assert!(inner.base.count > 0);
            if split_inner && inner.is_full() {
                counter!(metric::Counter::BtreeInnerSplit);
                let w_p = if let Some(p) = parent {
                    let w_p = p.0.upgrade().map_err(|(_, e)| e)?;
                    Some(w_p)
                } else {
                    None
                };

                let mut w_n = node.upgrade().map_err(|(_, e)| e)?;

                if w_p.is_none() && w_n.as_ref().pid() != self.get_root_pid() {
                    return Err(TreeError::NeedRestart);
                }

                match w_p {
                    Some(mut p) => {
                        let inner_mut = w_n.as_mut().as_inner_mut::<K>();
                        let (new_inner, sep) = inner_mut.split(
                            self.mem_src
                                .reserve_page(PageType::Inner)
                                .map_err(TreeError::OutOfSpace)?,
                        );
                        p.as_mut()
                            .as_inner_mut()
                            .insert(sep, new_inner.as_ref().pid());
                    }
                    None => {
                        let inner_mut = w_n.as_mut().as_inner_mut();
                        let new_node = self
                            .mem_src
                            .reserve_page(PageType::Inner)
                            .map_err(TreeError::OutOfSpace)?;

                        let new_root = match self.mem_src.reserve_page(PageType::Inner) {
                            Ok(v) => v,
                            Err(e) => {
                                self.mem_src.free_page(new_node.as_ref().pid(), new_node);
                                return Err(TreeError::OutOfSpace(e));
                            }
                        };
                        let (new_inner, sep) = inner_mut.split(new_node);
                        self.install_root(
                            sep,
                            w_n.as_ref().pid(),
                            new_inner.as_ref().pid(),
                            new_root,
                        );
                    }
                }
                counter!(metric::Counter::BtreeInnerSplitSuccess);
                return Err(TreeError::NeedRestart);
            }

            if let Some(p) = parent {
                p.0.check_version()?;
            }

            let pos = inner.lower_bound(key);
            let node_next = self.resolve_pid(inner.children[pos].clone()); // here we need a clone because current node might be evicted
            node.check_version()?;

            parent = Some((node, pos));

            node = node_next?;
        }

        Ok((node, parent))
    }

    pub fn pin(&self) -> crossbeam_epoch::Guard {
        crossbeam_epoch::pin()
    }

    pub fn get(&self, key: &K, guard: &crossbeam_epoch::Guard) -> Option<V> {
        self.compute_if_present(key, |v| v.clone(), guard)
            .map(|(_old, new)| new)
    }

    fn traverse_to_leaf_random(
        &self,
        rng: &mut impl rand::Rng,
    ) -> Result<(ReadGuard, Option<(ReadGuard, usize)>), TreeError> {
        let mut node = self.get_root()?;
        if node.as_ref().pid() != self.get_root_pid() {
            return Err(TreeError::NeedRestart);
        }

        let mut parent: Option<(ReadGuard, usize)> = None;

        while node.as_ref().page_type == PageType::Inner {
            let inner: &BtreeInner<K> = node.as_ref().as_inner();

            debug_assert!(inner.base.count > 0);

            if let Some(p) = parent {
                p.0.check_version()?;
            }

            let pos = rng.gen_range(0..inner.base.count) as usize;
            let node_next = self.resolve_pid(inner.children[pos].clone()); // here we need a clone because current node might be evicted
            node.check_version()?;

            parent = Some((node, pos));

            node = node_next?;
        }

        Ok((node, parent))
    }

    pub fn compute_n_on_key(
        &self,
        n: usize,
        key: &K,
        mut f: impl FnMut(&K, &mut V),
        _guard: &crossbeam_epoch::Guard,
    ) {
        let backoff = Backoff::new();
        'restart: loop {
            let (node, parent) = if let Ok(v) = self.traverse_to_leaf(key, false) {
                v
            } else {
                backoff.spin();
                continue 'restart;
            };

            let mut w_n = if let Ok(v) = node.upgrade() {
                v
            } else {
                backoff.spin();
                continue 'restart;
            };

            if let Some(p) = parent {
                if p.0.check_version().is_err() {
                    backoff.spin();
                    continue 'restart;
                }
            }

            let w_leaf = w_n.as_mut().as_leaf_mut::<K, V>();
            for i in 0..std::cmp::min(n, w_leaf.base.count as usize) {
                f(&w_leaf.keys[i], &mut w_leaf.values[i]);
            }
            return;
        }
    }

    pub fn compute_on_n_random<F>(
        &self,
        n: usize,
        mut f: F,
        rng: &mut impl rand::Rng,
        _guard: &Guard,
    ) where
        F: FnMut(&K, &mut V),
    {
        let backoff = Backoff::new();
        'restart: loop {
            let (node, parent) = if let Ok(v) = self.traverse_to_leaf_random(rng) {
                v
            } else {
                backoff.spin();
                continue 'restart;
            };

            let mut w_n = if let Ok(v) = node.upgrade() {
                v
            } else {
                backoff.spin();
                continue 'restart;
            };

            if let Some(p) = parent {
                if p.0.check_version().is_err() {
                    backoff.spin();
                    continue 'restart;
                }
            }

            let w_leaf = w_n.as_mut().as_leaf_mut::<K, V>();
            for i in 0..std::cmp::min(n, w_leaf.base.count as usize) {
                f(&w_leaf.keys[i], &mut w_leaf.values[i]);
            }
            return;
        }
    }

    /// Compute and update the value if the key presents in the tree.
    /// Returns the (old, new) value
    ///
    /// Note that the function `f` is a FnMut and it must be safe to execute multiple times.
    /// The `f` is expected to be short and fast as it will hold a exclusive lock on the leaf node.
    ///
    pub fn compute_if_present<F>(
        &self,
        key: &K,
        mut f: F,
        _guard: &crossbeam_epoch::Guard,
    ) -> Option<(V, V)>
    where
        F: FnMut(&V) -> V,
    {
        let backoff = Backoff::new();
        'restart: loop {
            let (node, parent) = if let Ok(v) = self.traverse_to_leaf(key, false) {
                v
            } else {
                backoff.spin();
                continue 'restart;
            };

            let leaf = node.as_ref().as_leaf::<K, V>();

            let pos = leaf.lower_bound(key);
            if pos < leaf.base.count as usize && leaf.keys[pos] == *key {
                let val = leaf.values[pos].clone();
                let new_v = f(&val);
                if new_v == val {
                    if let Some(p) = parent {
                        if p.0.check_version().is_err() {
                            backoff.spin();
                            continue 'restart;
                        }
                    }

                    if node.check_version().is_err() {
                        backoff.spin();
                        continue 'restart;
                    }
                    return Some((new_v, val));
                }

                let mut w_n = if let Ok(v) = node.upgrade() {
                    v
                } else {
                    backoff.spin();
                    continue 'restart;
                };

                if let Some(p) = parent {
                    if p.0.check_version().is_err() {
                        backoff.spin();
                        continue 'restart;
                    }
                }

                let w_leaf = w_n.as_mut().as_leaf_mut::<K, V>();
                w_leaf.values[pos] = new_v.clone();
                return Some((val, new_v));
            } else {
                if node.check_version().is_err() {
                    backoff.spin();
                    continue 'restart;
                }
                return None;
            }
        }
    }

    pub fn remove(&self, key: &K, _guard: &crossbeam_epoch::Guard) -> Option<V> {
        let backoff = Backoff::new();
        'restart: loop {
            let (node, parent) = if let Ok(v) = self.traverse_to_leaf(key, false) {
                v
            } else {
                backoff.spin();
                continue 'restart;
            };

            let leaf = node.as_ref().as_leaf::<K, V>();

            let pos = leaf.lower_bound(key);

            if pos < leaf.base.count as usize && leaf.keys[pos] == *key {
                let mut w_n = if let Ok(v) = node.upgrade() {
                    v
                } else {
                    backoff.spin();
                    continue 'restart;
                };

                if let Some(p) = parent {
                    if p.0.check_version().is_err() {
                        backoff.spin();
                        continue 'restart;
                    }
                }

                let leaf = w_n.as_mut().as_leaf_mut::<K, V>();
                let val = leaf.remove(pos);
                if leaf.base.count == 0 {
                    std::mem::drop(w_n);
                    while self.remove_leaf(key).is_err() {
                        backoff.spin();
                    }
                }
                return Some(val);
            } else {
                if node.check_version().is_err() {
                    backoff.spin();
                    continue 'restart;
                }
                return None;
            }
        }
    }

    fn remove_leaf(&self, key: &K) -> Result<(), TreeError> {
        let (leaf, parent) = self.traverse_to_leaf(key, false)?;
        let leaf_ref = leaf.as_ref().as_leaf::<K, V>();
        if leaf_ref.base.count > 0 {
            // Leaf is not empty (concurrent insert)
            return Ok(());
        }

        let p = match parent {
            Some(p) => p,
            None => return Ok(()), // we are root node, do nothing
        };

        if p.0.as_ref().count == 1 {
            // this is a compromise, we don't want to do structure modification during delete,
            // instead we keep the minimal amount of nodes that maintains current structure.
            counter!(metric::Counter::BtreeNodeRemoveCaveat);
            return Ok(());
        }
        counter!(metric::Counter::BtreeNodeRemove);

        let mut w_p = p.0.upgrade().map_err(|(_, e)| e)?;
        let w_p_inner = w_p.as_mut().as_inner_mut::<K>();

        assert!(w_p_inner.base.count > 0);

        let mut w_n = leaf.upgrade().map_err(|(_, e)| e)?;
        let w_leaf = w_n.as_mut().as_leaf_mut::<K, V>();
        let mut prev_node = if !w_leaf.prev.is_null() {
            Some(
                self.resolve_pid(w_leaf.prev.clone())?
                    .upgrade()
                    .map_err(|(_, e)| e)?,
            )
        } else {
            None
        };

        let mut next_node = if !w_leaf.next.is_null() {
            Some(
                self.resolve_pid(w_leaf.next.clone())?
                    .upgrade()
                    .map_err(|(_, e)| e)?,
            )
        } else {
            None
        };

        // we can only do the following after we acquired all the locks
        let old = w_p_inner.remove(p.1);
        debug_assert_eq!(
            self.resolve_pid(old.1.clone())?.as_ref() as *const BaseNode,
            &w_leaf.base
        );

        if let Some(ref mut prev_node) = prev_node {
            let old = std::mem::replace(&mut w_leaf.next, Pid::from_null());
            prev_node.as_mut().as_leaf_mut::<K, V>().next = old;
        }
        if let Some(ref mut next_node) = next_node {
            let old = std::mem::replace(&mut w_leaf.prev, Pid::from_null());
            next_node.as_mut().as_leaf_mut::<K, V>().prev = old;
        }

        self.mem_src.free_page(old.1, w_n);

        Ok(())
    }

    pub fn range<'b>(
        &'b self,
        low: &'b K,
        high: &'b K,
        _guard: &crossbeam_epoch::Guard,
    ) -> BtreeIter<K, V, M> {
        BtreeIter::new(self, low, high)
    }
}
