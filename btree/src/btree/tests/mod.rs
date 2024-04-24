use std::sync::Arc;

use crate::PAGE_SIZE;

mod basic;
mod scan;

mod delete;

#[derive(PartialEq, Debug, Eq, PartialOrd, Ord, Clone)]
struct LargeKey {
    key: usize,
    _padding: [u8; 512],
}

impl LargeKey {
    fn new(key: usize) -> Self {
        Self {
            key,
            _padding: [0; 512],
        }
    }
}

#[cfg(feature = "inmemory")]
use crate::buffer_pool::InMemory;

#[cfg(feature = "inmemory")]
fn get_memory_source() -> Arc<DramSource> {
    Arc::new(DramSource {})
}

#[cfg(not(feature = "inmemory"))]
use crate::buffer_pool::ThreeTierBufferPool;

#[cfg(not(feature = "inmemory"))]
fn get_memory_source() -> Arc<ThreeTierBufferPool<1>> {
    Arc::new(ThreeTierBufferPool::new(
        PAGE_SIZE * 512,
        PAGE_SIZE * 512,
        true,
    ))
}
