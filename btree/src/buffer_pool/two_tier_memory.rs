use std::{
    alloc::Layout,
    sync::atomic::{AtomicIsize, AtomicUsize, Ordering},
};

use douhua::TieredAllocator;
use metric::{counter, Counter};
use rand::Rng;

use crate::{
    btree::{basic::BaseNode, lock::WriteGuard, PageType, NODE_LAYOUT},
    pid::Pid,
    utils::Backoff,
    PAGE_SIZE,
};

use super::{
    three_tier_bp::evict_to_remote_callback, utils::HashMap, BufferPool, OutOfSpaceError,
    PhysicalPage,
};

pub struct TwoTierMemory<const SHARD_CNT: usize> {
    pub(super) page_table: HashMap<Pid, PhysicalPage>,
    local_buffer: jasmine_db::ShardCache<{ SHARD_CNT }>,
    remote_remaining: AtomicIsize,
    current_page_id: AtomicUsize, // starts from 1
}

impl<const SHARD_CNT: usize> BufferPool for TwoTierMemory<SHARD_CNT> {
    fn reserve_page<'a>(&'a self, page_type: PageType) -> Result<WriteGuard<'_>, OutOfSpaceError> {
        let new_page_id = self
            .current_page_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pid = Pid::from(new_page_id);

        let fill_callback = |evict_rt: Option<WriteGuard<'a>>, ptr: *mut u8| -> WriteGuard<'a> {
            match evict_rt {
                Some(mut wg) => {
                    let node_mut = wg.as_mut();
                    node_mut.count = 0;
                    node_mut.is_dirty = false;
                    node_mut.pid = new_page_id;
                    node_mut.page_type = page_type.clone();
                    wg
                }
                None => {
                    let ptr = ptr as *mut BaseNode;
                    let base = BaseNode::new(new_page_id, page_type.clone(), douhua::MemType::DRAM);
                    unsafe {
                        std::ptr::write(ptr, base);
                    }
                    BaseNode::write_lock_new(ptr).expect("No one should content with us.")
                }
            }
        };

        if let Ok(mut wg) = self
            .local_buffer
            .get()
            .probe_entry_evict(|_| None, fill_callback)
        {
            let ptr = wg.as_mut();
            self.page_table.insert(pid, PhysicalPage::LocalMemory(ptr));
            return Ok(wg);
        };

        // Local memory is full, try remote memory.
        let old_size = self
            .remote_remaining
            .fetch_sub(NODE_LAYOUT.size() as isize, Ordering::Relaxed);
        if old_size < NODE_LAYOUT.size() as isize {
            self.remote_remaining
                .fetch_add(NODE_LAYOUT.size() as isize, Ordering::Relaxed);
            return Err(OutOfSpaceError::new(page_type));
        }

        let ptr = douhua::Allocator::get()
            .allocate_zeroed(NODE_LAYOUT, douhua::MemType::NUMA)
            .map_err(|_e| {
                self.remote_remaining
                    .fetch_add(NODE_LAYOUT.size() as isize, Ordering::Relaxed);
                OutOfSpaceError::new(page_type.clone())
            })?;
        let base = BaseNode::new(new_page_id, page_type, douhua::MemType::NUMA);
        let ptr = ptr.as_non_null_ptr().as_ptr() as *mut BaseNode;
        let wg = unsafe {
            std::ptr::write(ptr, base);
            BaseNode::write_lock_new(ptr).expect("no contention here")
        };
        self.page_table.insert(pid, PhysicalPage::TieredMemory(ptr));
        Ok(wg)
    }

    fn free_page(&self, pid: Pid, mut wg: WriteGuard) {
        let expected_ptr = wg.as_mut() as *mut BaseNode;
        wg.mark_obsolete();
        std::mem::forget(wg);

        counter!(Counter::FreePage);

        let old = self.page_table.get(&pid).expect("The value must exists.");
        // TODO: we need to delete this pid from the page table, to improve page table efficiency.
        // We can only delete this page in the epoch guard, which is not easy with current epoch guard.

        match *old.value() {
            PhysicalPage::LocalMemory(ptr) => unsafe {
                assert_eq!(ptr, expected_ptr);
                self.local_buffer.mark_empty(ptr as *const u8);
            },
            PhysicalPage::TieredMemory(_ptr) => {
                self.remote_remaining
                    .fetch_add(NODE_LAYOUT.size() as isize, Ordering::Relaxed);
            }
            _ => {
                unreachable!()
            }
        }
    }

    fn resolve_pid(&self, pid: Pid) -> *mut BaseNode {
        let backoff = Backoff::new();
        loop {
            let page_ref = self.page_table.get(&pid).expect("The value must exists.");
            let page = page_ref.value().clone(); // to release the lock;
            std::mem::drop(page_ref);
            match page {
                PhysicalPage::LocalMemory(ptr) => {
                    unsafe { self.local_buffer.mark_referenced(ptr as *const u8) };
                    if unsafe { &*ptr }.page_type == PageType::Leaf {
                        counter!(metric::Counter::BtreeLeafLocalHit);
                    }
                    counter!(metric::Counter::BtreeLocalHit);
                    return ptr;
                }
                PhysicalPage::TieredMemory(ptr) => {
                    crate::utils::remote_delay();
                    if unsafe { &*ptr }.page_type == PageType::Leaf {
                        counter!(metric::Counter::BtreeLeafRemoteHit);
                    }
                    counter!(Counter::BtreeRemoteHit);
                    if rand::thread_rng().gen_range(0..100) <= crate::utils::get_promotion_rate() {
                        if let Some(ptr) = self.promote_tiered_page(&pid, ptr) {
                            return ptr;
                        } else {
                            return ptr;
                        }
                    } else {
                        return ptr;
                    }
                }
                PhysicalPage::Inflight => {
                    backoff.snooze();
                    continue;
                }
                _ => unreachable!(),
            }
        }
    }
}

impl<const SHARD_CNT: usize> TwoTierMemory<SHARD_CNT> {
    pub fn new(local_buffer_byte: isize, remote_buffer_byte: isize) -> Self {
        TwoTierMemory {
            page_table: HashMap::new(),
            local_buffer: jasmine_db::ShardCache::new(
                local_buffer_byte as usize,
                Layout::from_size_align(PAGE_SIZE, PAGE_SIZE).unwrap(),
                douhua::MemType::DRAM,
            ),
            remote_remaining: AtomicIsize::new(remote_buffer_byte),
            current_page_id: AtomicUsize::new(1),
        }
    }

    fn promote_tiered_page(
        &self,
        pid: &Pid,
        page_on_tiered: *mut BaseNode,
    ) -> Option<*mut BaseNode> {
        let page = PhysicalPage::TieredMemory(page_on_tiered);
        let locked_page = match BaseNode::write_lock_new(page_on_tiered) {
            Ok(locked_page) => locked_page,
            Err(_) => return Some(page_on_tiered),
        };
        self.page_table
            .compare_exchange(pid, &page, PhysicalPage::Inflight)
            .ok()?;
        std::mem::forget(locked_page);

        let evict_buffer = unsafe { std::alloc::alloc_zeroed(NODE_LAYOUT) } as *mut BaseNode;

        loop {
            match self.local_buffer.get().probe_entry_evict(
                |ptr| {
                    evict_to_remote_callback::<{ SHARD_CNT }>(ptr, evict_buffer, &self.page_table)
                },
                |evicted, dst: *mut u8| {
                    let dst = dst as *mut BaseNode;
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            page_on_tiered as *const u8,
                            dst as *mut u8,
                            PAGE_SIZE,
                        );
                    }
                    (evicted, dst)
                },
            ) {
                Ok((evicted, new_p)) => {
                    self.page_table
                        .compare_exchange(
                            pid,
                            &PhysicalPage::Inflight,
                            PhysicalPage::LocalMemory(new_p),
                        )
                        .expect("No one else should be able to change the page.");
                    unsafe { (*new_p).unlock() };
                    if evicted.is_some() {
                        let evict_pid = unsafe { &*evict_buffer }.pid();
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                evict_buffer as *const u8,
                                page_on_tiered as *mut u8,
                                PAGE_SIZE,
                            );
                        }
                        self.page_table
                            .compare_exchange(
                                &evict_pid,
                                &PhysicalPage::Inflight,
                                PhysicalPage::TieredMemory(page_on_tiered),
                            )
                            .expect("No one else should be able to change the page.");
                        unsafe { (*page_on_tiered).unlock() };
                    }
                    unsafe { std::alloc::dealloc(evict_buffer as *mut u8, NODE_LAYOUT) };
                    counter!(metric::Counter::RemoteToLocalOk);
                    return Some(new_p);
                }
                Err(_e) => {
                    counter!(metric::Counter::RemoteToLocalFailed);
                    unsafe { (*page_on_tiered).unlock() };
                    self.page_table
                        .compare_exchange(pid, &PhysicalPage::Inflight, page)
                        .ok()?;
                    return Some(page_on_tiered);
                }
            }
        }
    }
}
