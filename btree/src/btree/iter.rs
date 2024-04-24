use std::fmt::Debug;

use super::{inner::btree_inner_entries, leaf::btree_leaf_entries, lock::ReadGuard, Btree};
use crate::{buffer_pool::BufferPool, utils::Backoff};

pub struct BtreeIter<
    'tree,
    K: PartialOrd + Clone + Debug,
    V: Clone + PartialEq,
    M: BufferPool + 'static,
> where
    [(); btree_leaf_entries::<K, V>()]:,
    [(); btree_inner_entries::<K>()]:,
{
    tree: &'tree Btree<K, V, M>,
    node: ReadGuard<'tree>,
    cur_idx: usize,
    last_key: K,
    high: &'tree K,
}

impl<'tree, K: PartialOrd + Clone + Debug, V: Clone + PartialEq, M: BufferPool>
    BtreeIter<'tree, K, V, M>
where
    [(); btree_leaf_entries::<K, V>()]:,
    [(); btree_inner_entries::<K>()]:,
{
    pub(crate) fn new(tree: &'tree Btree<K, V, M>, low: &'tree K, high: &'tree K) -> Self {
        let backoff = Backoff::new();
        loop {
            let (leaf, _parent) = if let Ok(v) = tree.traverse_to_leaf(low, false) {
                v
            } else {
                backoff.spin();
                continue;
            };
            let idx = leaf.as_ref().as_leaf::<K, V>().lower_bound(low);

            return BtreeIter {
                tree,
                node: leaf,
                cur_idx: idx,
                last_key: low.clone(),
                high,
            };
        }
    }
}

impl<'tree, K: PartialOrd + Clone + Debug, V: Clone + PartialEq, M: BufferPool> Iterator
    for BtreeIter<'tree, K, V, M>
where
    [(); btree_leaf_entries::<K, V>()]:,
    [(); btree_inner_entries::<K>()]:,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        let backoff = Backoff::new();
        'need_restart: loop {
            match self.node.check_version() {
                Err(_) => {
                    let (leaf, _parent) =
                        if let Ok(v) = self.tree.traverse_to_leaf(&self.last_key, false) {
                            v
                        } else {
                            backoff.spin();
                            continue 'need_restart;
                        };
                    self.node = leaf;
                    continue 'need_restart;
                }
                Ok(_) => {
                    if self.cur_idx < self.node.as_ref().count as usize {
                        let key = self.node.as_ref().as_leaf::<K, V>().keys[self.cur_idx].clone();
                        if key < *self.high {
                            let value =
                                self.node.as_ref().as_leaf::<K, V>().values[self.cur_idx].clone();
                            self.cur_idx += 1;
                            self.last_key = key.clone();
                            return Some((key, value));
                        } else {
                            return None;
                        }
                    } else {
                        let next = &self.node.as_ref().as_leaf::<K, V>().next;
                        if next.is_null() {
                            return None;
                        }
                        self.node = if let Ok(v) = self.tree.resolve_pid(next.clone()) {
                            v
                        } else {
                            backoff.spin();
                            continue 'need_restart;
                        };
                        self.cur_idx = 0;
                        // no spin here
                        continue 'need_restart;
                    }
                }
            }
        }
    }
}
