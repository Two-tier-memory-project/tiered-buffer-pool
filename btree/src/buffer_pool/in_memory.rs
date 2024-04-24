use std::sync::{
    atomic::{AtomicIsize, Ordering},
    Arc,
};

use douhua::{MemType, TieredAllocator};
use metric::{counter, Counter};

use crate::{
    btree::{basic::BaseNode, lock::WriteGuard, PageType, NODE_LAYOUT},
    buffer_pool::BufferPool,
};

use super::OutOfSpaceError;

pub struct InMemory {
    mem_type: MemType,
    max_size: AtomicIsize,
}

impl Default for InMemory {
    fn default() -> Self {
        Self {
            mem_type: MemType::DRAM,
            max_size: AtomicIsize::new(isize::MAX),
        }
    }
}

impl InMemory {
    pub fn new(mem_type: MemType, max_size: isize) -> Self {
        Self {
            mem_type,
            max_size: AtomicIsize::new(max_size),
        }
    }
}

impl BufferPool for Arc<InMemory> {
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

impl BufferPool for InMemory {
    fn reserve_page(&self, page_type: PageType) -> Result<WriteGuard, OutOfSpaceError> {
        let old_size = self
            .max_size
            .fetch_sub(NODE_LAYOUT.size() as isize, Ordering::Relaxed);
        if old_size < NODE_LAYOUT.size() as isize {
            self.max_size
                .fetch_add(NODE_LAYOUT.size() as isize, Ordering::Relaxed);
            return Err(OutOfSpaceError::new(page_type));
        }

        counter!(Counter::ReserveInMemoryPage);

        let mut base = BaseNode::new(0, page_type, self.mem_type);
        let ptr = douhua::Allocator::get()
            .allocate_zeroed(NODE_LAYOUT, self.mem_type)
            .expect("failed to allocate memory");

        let ptr = ptr.as_non_null_ptr().as_ptr() as *mut BaseNode;
        base.pid = ptr as usize;
        unsafe {
            std::ptr::write(ptr, base);
            Ok(BaseNode::write_lock_new(ptr).expect("no contention here"))
        }
    }

    fn free_page(&self, pid: crate::pid::Pid, mut wg: WriteGuard) {
        let ptr = pid.id as *mut BaseNode;
        wg.mark_obsolete();
        std::mem::forget(wg);

        counter!(Counter::FreeInMemoryPage);

        self.max_size
            .fetch_add(NODE_LAYOUT.size() as isize, Ordering::Relaxed);
        let node_usize = ptr as usize;
        let mem_type = self.mem_type;
        let guard = crossbeam_epoch::pin();
        guard.defer(move || unsafe {
            let non_null = std::ptr::NonNull::new_unchecked(node_usize as *mut u8);
            douhua::Allocator::get().deallocate(non_null, NODE_LAYOUT, mem_type);
        });
    }

    fn resolve_pid(&self, pid: crate::pid::Pid) -> *mut BaseNode {
        let ptr = pid.id as *mut BaseNode;
        let rg = unsafe { &mut *ptr };

        if rg.page_type == PageType::Leaf {
            if rg.mem_type == MemType::NUMA {
                crate::utils::remote_delay();
                counter!(Counter::BtreeLeafRemoteHit);
            } else {
                counter!(Counter::BtreeLeafLocalHit);
            }
        }
        rg
    }
}
