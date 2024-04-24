#![allow(dead_code)]
/// This mod describes a set of allocators that can be used to bound memory usage.
/// Given a memory resource bound, e.g., the max local memory size, the allocator will
/// never allocate more than that amount of memory.
///
/// This is helpful for our benchmark suite, where we want to bound the memory usage of each design.
/// When memory is used up, the allocator returns AllocatorError::OutOfMemory.
///
/// The SimpleMemoryAllocator will only allocate memory from the given memory resource.
/// The BestEffortAllocator will try to allocate memory from local memory first, then remote memory.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use congee::CongeeAllocator;
use douhua::{MemType, TieredAllocator};

use crate::backoff::Backoff;

/// Thread-safe substract an AtomicUsize by a usize.
/// Returns true if the AtomicUsize is larger than usize, else returns Err(current_value)
fn subtract_atomicusize(target: &AtomicUsize, size: usize) -> Result<(), usize> {
    let mut current = target.load(Ordering::Acquire);
    let backoff = Backoff::new();
    loop {
        if current < size {
            return Err(current);
        }
        match target.compare_exchange_weak(
            current,
            current - size,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(()),
            Err(o) => {
                current = o;
                backoff.spin();
            }
        }
    }
}

struct BestEffortMemAllocatorInner {
    local_remain: AtomicUsize,
    remote_remain: AtomicUsize,
}

#[derive(Clone)]
pub struct BestEffortMemAllocator(Arc<BestEffortMemAllocatorInner>);

impl BestEffortMemAllocator {
    pub(crate) fn new(local_remain: usize, remote_remain: usize) -> Self {
        Self(Arc::new(BestEffortMemAllocatorInner {
            local_remain: AtomicUsize::new(local_remain),
            remote_remain: AtomicUsize::new(remote_remain),
        }))
    }

    pub(crate) fn remaining_local_bytes(&self) -> usize {
        self.0.local_remain.load(Ordering::Acquire)
    }

    pub(crate) fn remaining_remote_bytes(&self) -> usize {
        self.0.remote_remain.load(Ordering::Acquire)
    }
}

unsafe impl CongeeAllocator for BestEffortMemAllocator {
    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<(std::ptr::NonNull<[u8]>, MemType), douhua::AllocError> {
        if subtract_atomicusize(&self.0.local_remain, layout.size()).is_ok() {
            let ptr = douhua::Allocator::get().allocate(layout, MemType::DRAM)?;
            return Ok((ptr, MemType::DRAM));
        };

        match subtract_atomicusize(&self.0.remote_remain, layout.size()) {
            Ok(_) => {
                let ptr = douhua::Allocator::get().allocate(layout, MemType::NUMA)?;
                Ok((ptr, MemType::NUMA))
            }
            Err(_) => Err(douhua::AllocError::OutOfMemory),
        }
    }

    unsafe fn deallocate(
        &self,
        ptr: std::ptr::NonNull<u8>,
        layout: std::alloc::Layout,
        mem_type: douhua::MemType,
    ) {
        douhua::Allocator::get().deallocate(ptr, layout, mem_type);
        match mem_type {
            MemType::DRAM => self
                .0
                .local_remain
                .fetch_add(layout.size(), Ordering::Release),
            MemType::NUMA => self
                .0
                .remote_remain
                .fetch_add(layout.size(), Ordering::Release),
        };
    }
}

struct SimpleMemAllocatorInner {
    remaining: AtomicUsize,
    mem_type: MemType,
}

#[derive(Clone)]
pub struct SimpleMemAllocator(Arc<SimpleMemAllocatorInner>);

impl SimpleMemAllocator {
    pub(crate) fn new(mem_type: MemType, mem_size: usize) -> Self {
        Self(Arc::new(SimpleMemAllocatorInner {
            remaining: AtomicUsize::new(mem_size),
            mem_type,
        }))
    }

    pub(crate) fn remaining_bytes(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }
}

unsafe impl CongeeAllocator for SimpleMemAllocator {
    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<(std::ptr::NonNull<[u8]>, douhua::MemType), douhua::AllocError> {
        match subtract_atomicusize(&self.0.remaining, layout.size()) {
            Ok(_) => {
                let ptr = douhua::Allocator::get().allocate(layout, self.0.mem_type)?;
                Ok((ptr, self.0.mem_type))
            }
            Err(_) => Err(douhua::AllocError::OutOfMemory),
        }
    }

    unsafe fn deallocate(
        &self,
        ptr: std::ptr::NonNull<u8>,
        layout: std::alloc::Layout,
        mem_type: douhua::MemType,
    ) {
        assert_eq!(mem_type, self.0.mem_type);
        douhua::Allocator::get().deallocate(ptr, layout, mem_type);
        self.0.remaining.fetch_add(layout.size(), Ordering::Relaxed);
    }
}
