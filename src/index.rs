use std::{fmt::Debug, sync::Arc};

use btree_olc::{btree::btree_leaf_entries, buffer_pool::InMemory};
use crossbeam_epoch::Guard;
use douhua::MemType;

use crate::alloc::SimpleMemAllocator;

pub trait InMemoryIndex<V>: Send + Sync {
    fn new(mem_type: MemType) -> Self;
    fn get(&self, k: &usize, guard: &Guard) -> Option<V>;
    fn insert(&self, k: usize, v: V, guard: &Guard);
    fn remove(&self, k: &usize, guard: &Guard) -> Option<V>;
    fn pin(&self) -> Guard;
}

impl<V: Clone + PartialEq + Send + Sync + Debug> InMemoryIndex<V>
    for btree_olc::btree::Btree<usize, V, InMemory>
where
    [(); btree_leaf_entries::<usize, V>()]:,
{
    fn new(mem_type: MemType) -> Self {
        btree_olc::btree::Btree::new(Arc::new(InMemory::new(mem_type, isize::MAX)))
    }

    fn get(&self, k: &usize, guard: &Guard) -> Option<V> {
        self.get(k, guard)
    }

    fn insert(&self, k: usize, v: V, guard: &Guard) {
        self.insert(k, v, guard).unwrap();
    }

    fn pin(&self) -> Guard {
        self.pin()
    }

    fn remove(&self, k: &usize, guard: &Guard) -> Option<V> {
        self.remove(k, guard)
    }
}

impl<V: Clone + From<usize> + Send + Sync> InMemoryIndex<V>
    for congee::Art<usize, V, SimpleMemAllocator>
where
    usize: From<V>,
{
    fn new(mem_type: MemType) -> Self {
        let allocator = SimpleMemAllocator::new(mem_type, usize::MAX); // size dones't matter for now.
        congee::Art::<usize, V, SimpleMemAllocator>::new(allocator)
    }

    fn get(&self, k: &usize, guard: &Guard) -> Option<V> {
        self.get(k, guard)
    }

    fn insert(&self, k: usize, v: V, guard: &Guard) {
        self.insert(k, v, guard).unwrap();
    }

    fn pin(&self) -> Guard {
        self.pin()
    }

    fn remove(&self, k: &usize, guard: &Guard) -> Option<V> {
        self.remove(k, guard)
    }
}
