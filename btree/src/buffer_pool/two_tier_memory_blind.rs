use std::sync::{
    atomic::{AtomicIsize, Ordering},
    Arc,
};

use douhua::{MemType, TieredAllocator};
use metric::{counter, Counter};

use crate::{
    btree::{basic::BaseNode, lock::WriteGuard, PageType, NODE_LAYOUT},
    buffer_pool::OutOfSpaceError,
};

use super::BufferPool;

pub struct TwoTierMemoryBlind {
    local_remaining: AtomicIsize,
    remote_remaining: AtomicIsize,
}

impl TwoTierMemoryBlind {
    pub fn new(local_byte: isize, remote_byte: isize) -> Self {
        Self {
            local_remaining: AtomicIsize::new(local_byte),
            remote_remaining: AtomicIsize::new(remote_byte),
        }
    }
}

impl BufferPool for Arc<TwoTierMemoryBlind> {
    fn reserve_page(&self, page_type: PageType) -> Result<WriteGuard, OutOfSpaceError> {
        self.as_ref().reserve_page(page_type)
    }

    fn resolve_pid(&self, pid: crate::pid::Pid) -> *mut BaseNode {
        self.as_ref().resolve_pid(pid)
    }

    fn free_page(&self, pid: crate::pid::Pid, wg: WriteGuard) {
        self.as_ref().free_page(pid, wg)
    }
}

impl TwoTierMemoryBlind {
    fn try_get_page(&self) -> Option<MemType> {
        // Try local
        let old_local = self
            .local_remaining
            .fetch_sub(NODE_LAYOUT.size() as isize, Ordering::Relaxed);
        if old_local > NODE_LAYOUT.size() as isize {
            return Some(MemType::DRAM);
        }
        self.local_remaining
            .fetch_add(NODE_LAYOUT.size() as isize, Ordering::Relaxed);

        // Try remote
        let old_remote = self
            .remote_remaining
            .fetch_sub(NODE_LAYOUT.size() as isize, Ordering::Relaxed);
        if old_remote > NODE_LAYOUT.size() as isize {
            return Some(MemType::NUMA);
        }
        self.remote_remaining
            .fetch_add(NODE_LAYOUT.size() as isize, Ordering::Relaxed);
        None
    }
}

impl BufferPool for TwoTierMemoryBlind {
    fn reserve_page(&self, page_type: PageType) -> Result<WriteGuard, OutOfSpaceError> {
        let mem_type = self
            .try_get_page()
            .ok_or(OutOfSpaceError::new(page_type.clone()))?;
        counter!(Counter::ReservePage);

        let mut base = BaseNode::new(0, page_type, mem_type);
        let ptr = douhua::Allocator::get()
            .allocate_zeroed(NODE_LAYOUT, mem_type)
            .expect("failed to allocate memory");

        let ptr = ptr.as_non_null_ptr().as_ptr() as *mut BaseNode;
        base.pid = ptr as usize;
        unsafe {
            std::ptr::write(ptr, base);
            Ok(BaseNode::write_lock_new(ptr).expect("no contention here"))
        }
    }

    fn free_page(&self, pid: crate::pid::Pid, mut wg: WriteGuard) {
        let mem_type = wg.as_mut().mem_type;
        let ptr = pid.id as *mut BaseNode;
        wg.mark_obsolete();
        std::mem::forget(wg);

        counter!(Counter::FreePage);

        match mem_type {
            MemType::DRAM => self
                .local_remaining
                .fetch_add(NODE_LAYOUT.size() as isize, Ordering::Relaxed),
            MemType::NUMA => self
                .remote_remaining
                .fetch_add(NODE_LAYOUT.size() as isize, Ordering::Relaxed),
        };
        let node_usize = ptr as usize;
        let guard = crossbeam_epoch::pin();
        guard.defer(move || unsafe {
            let non_null = std::ptr::NonNull::new_unchecked(node_usize as *mut u8);
            douhua::Allocator::get().deallocate(non_null, NODE_LAYOUT, mem_type);
        });
    }

    fn resolve_pid(&self, pid: crate::pid::Pid) -> *mut BaseNode {
        let ptr = pid.id as *mut BaseNode;
        let rg = unsafe { &mut *ptr };

        if rg.mem_type == MemType::NUMA {
            crate::utils::remote_delay();
        }

        if rg.page_type == PageType::Leaf {
            if rg.mem_type == MemType::NUMA {
                counter!(Counter::BtreeLeafRemoteHit);
            } else {
                counter!(Counter::BtreeLeafLocalHit);
            }
        }
        rg
    }
}
