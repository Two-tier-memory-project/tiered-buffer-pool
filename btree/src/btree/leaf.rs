use crate::pid::Pid;

use super::{basic::BaseNode, lock::WriteGuard, PAGE_SIZE};

pub const fn btree_leaf_entries<Key, Value>() -> usize {
    (PAGE_SIZE - std::mem::size_of::<BaseNode>() - std::mem::size_of::<Pid>() * 2)
        / (std::mem::size_of::<Key>() + std::mem::size_of::<Value>())
}

#[repr(C)]
pub(crate) struct BtreeLeaf<K: PartialOrd + Clone, V>
where
    [(); btree_leaf_entries::<K, V>()]:,
{
    pub(crate) base: BaseNode,
    pub(crate) next: Pid,
    pub(crate) prev: Pid,
    pub(crate) keys: [K; btree_leaf_entries::<K, V>()],
    pub(crate) values: [V; btree_leaf_entries::<K, V>()],
}

impl<K: PartialOrd + Clone, V: Clone> BtreeLeaf<K, V>
where
    [(); btree_leaf_entries::<K, V>()]:,
{
    pub(crate) fn is_full(&self) -> bool {
        self.base.count == btree_leaf_entries::<K, V>() as u16
    }

    pub(crate) fn lower_bound(&self, key: &K) -> usize {
        let mut lower = 0;
        let mut upper = self.base.count as usize;

        loop {
            let mid = (upper + lower) / 2;
            if *key < self.keys[mid] {
                upper = mid;
            } else if *key > self.keys[mid] {
                lower = mid + 1;
            } else {
                return mid;
            }
            if lower >= upper {
                return lower;
            }
        }
    }

    /// Retruns the previous old value, if exsits.
    pub(crate) fn insert(&mut self, key: K, payload: V) -> Option<V> {
        debug_assert!(self.base.count < btree_leaf_entries::<K, V>() as u16);
        if self.base.count > 0 {
            let pos = self.lower_bound(&key);
            if (pos < self.base.count as usize) && (self.keys[pos] == key) {
                // upsert
                let old = std::mem::replace(&mut self.values[pos], payload);
                return Some(old);
            }
            unsafe {
                std::ptr::copy(
                    self.keys.as_mut_ptr().add(pos),
                    self.keys.as_mut_ptr().add(pos + 1),
                    self.base.count as usize - pos,
                );
                std::ptr::copy(
                    self.values.as_mut_ptr().add(pos),
                    self.values.as_mut_ptr().add(pos + 1),
                    self.base.count as usize - pos,
                );
            }
            self.keys[pos] = key;
            self.values[pos] = payload;
        } else {
            self.keys[0] = key;
            self.values[0] = payload;
        }
        self.base.count += 1;
        None
    }

    pub(crate) fn remove(&mut self, pos: usize) -> V {
        debug_assert!(pos < self.base.count as usize);
        debug_assert!(self.base.count > 0);
        let val = unsafe { std::ptr::read(self.values.as_ptr().add(pos)) };
        unsafe {
            std::ptr::copy(
                self.keys.as_mut_ptr().add(pos + 1),
                self.keys.as_mut_ptr().add(pos),
                self.base.count as usize - pos - 1,
            );
            std::ptr::copy(
                self.values.as_mut_ptr().add(pos + 1),
                self.values.as_mut_ptr().add(pos),
                self.base.count as usize - pos - 1,
            );
        }
        self.base.count -= 1;
        val
    }

    pub(crate) fn split<'a>(
        &'a mut self,
        leaf_next: &mut Option<WriteGuard>,
        mut new_node: WriteGuard<'a>,
    ) -> (WriteGuard, K) {
        let new_node_leaf = new_node.as_mut().as_leaf_mut();
        new_node_leaf.base.count = self.base.count / 2;
        self.base.count -= new_node_leaf.base.count;

        unsafe {
            std::ptr::copy_nonoverlapping(
                self.keys.as_ptr().offset(self.base.count as isize),
                new_node_leaf.keys.as_mut_ptr(),
                new_node_leaf.base.count as usize,
            );
            std::ptr::copy_nonoverlapping(
                self.values.as_ptr().offset(self.base.count as isize),
                new_node_leaf.values.as_mut_ptr(),
                new_node_leaf.base.count as usize,
            )
        }
        let old = std::mem::replace(&mut self.next, new_node_leaf.base.pid());
        new_node_leaf.next = old;
        new_node_leaf.prev = self.base.pid();
        if let Some(ref mut next) = *leaf_next {
            next.as_mut().as_leaf_mut::<K, V>().prev = new_node_leaf.base.pid();
        }

        (new_node, self.keys[self.base.count as usize - 1].clone())
    }
}
