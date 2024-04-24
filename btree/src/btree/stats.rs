use crate::buffer_pool::BufferPool;
use std::fmt::Debug;

use super::{btree_inner_entries, btree_leaf_entries, Btree, BtreeInner, PageType};

impl<K: PartialOrd + Clone + Debug, V: Clone + PartialEq, M: BufferPool> Btree<K, V, M>
where
    [(); btree_leaf_entries::<K, V>()]:,
    [(); btree_inner_entries::<K>()]:,
{
    /// Get tree stats for diagnostics.
    /// This is a long operation, so it should not be called frequently.
    pub fn stats(&mut self) -> BtreeStats {
        let mut levels: Vec<BtreeLevelStats> = Vec::new();
        let mut sub_nodes = vec![(0, self.get_root_pid())];

        while let Some((level, pid)) = sub_nodes.pop() {
            if levels.len() <= level {
                levels.push(BtreeLevelStats::new(level + 1));
            }
            let node = self
                .resolve_pid(pid)
                .expect("Can't fail when we have mutable reference");
            match node.as_ref().page_type {
                PageType::Inner => {
                    levels[level].node_cnt += 1;
                    levels[level].entry_cnt += node.as_ref().count as usize;
                    for i in 0..(node.as_ref().count + 1) {
                        let inner: &BtreeInner<K> = node.as_ref().as_inner();
                        let entry = inner.children[i as usize].clone();
                        sub_nodes.push((level + 1, entry));
                    }
                }
                PageType::Leaf => {
                    levels[level].node_cnt += 1;
                    levels[level].entry_cnt += node.as_ref().count as usize;
                }
            }
        }
        BtreeStats::new(
            btree_leaf_entries::<K, V>(),
            btree_inner_entries::<K>(),
            levels,
        )
    }
}

#[derive(serde::Serialize, Debug)]
pub struct BtreeStats {
    pub levels: Vec<BtreeLevelStats>,
    pub leaf_capacity: usize,
    pub inner_capacity: usize,
    pub load_factor: f64,
    pub total_node_cnt: usize,
}

impl BtreeStats {
    pub fn new(
        leaf_capacity: usize,
        inner_capacity: usize,
        mut levels: Vec<BtreeLevelStats>,
    ) -> Self {
        let total_level = levels.len();
        let mut node_cnt = 0;
        let mut node_load_factor = 0.0;
        for level in levels.iter_mut() {
            if level.level != total_level {
                level.set_load_factor(inner_capacity);
            } else {
                level.set_load_factor(leaf_capacity);
            }
            node_cnt += level.node_cnt;
            node_load_factor += level.load_factor * level.node_cnt as f64;
        }
        Self {
            levels,
            leaf_capacity,
            inner_capacity,
            load_factor: node_load_factor / node_cnt as f64,
            total_node_cnt: node_cnt,
        }
    }
}

#[derive(serde::Serialize, Debug)]
pub struct BtreeLevelStats {
    pub level: usize,
    pub node_cnt: usize,
    pub entry_cnt: usize,
    pub load_factor: f64,
}

impl BtreeLevelStats {
    fn new(level: usize) -> Self {
        Self {
            level,
            node_cnt: 0,
            entry_cnt: 0,
            load_factor: 0.0,
        }
    }

    fn set_load_factor(&mut self, capacity_per_node: usize) {
        self.load_factor =
            self.entry_cnt as f64 / (self.node_cnt as f64 * capacity_per_node as f64);
    }
}
