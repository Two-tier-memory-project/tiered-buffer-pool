#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::AtomicUsize;

use std::sync::Arc;

use metric::counter;
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::AtomicUsize;

use jasmine_db::JasmineError;
use rand::Rng;

use crate::{
    btree::{basic::BaseNode, lock::WriteGuard, PageType, NODE_LAYOUT},
    buffer_pool::BufferPool,
    pid::Pid,
    utils::{Backoff, TreeError},
    PAGE_SIZE,
};

use super::{
    utils::HashMap,
    vfs::{self, VFS},
    OutOfSpaceError, PhysicalPage,
};

pub struct ThreeTierBufferPool<const SHARD_CNT: usize> {
    pub(super) page_table: HashMap<Pid, PhysicalPage>,
    local_buffer: jasmine_db::ShardCache<{ SHARD_CNT }>,
    remote_buffer: jasmine_db::ShardCache<{ SHARD_CNT }>,
    direct_file: vfs::FastLoadFile,
    // direct_file: vfs::io_uring_file::IoUringFile,
    current_page_id: AtomicUsize, // starts from 1
    promote_remote: bool,
}

impl<const SHARD_CNT: usize> BufferPool for Arc<ThreeTierBufferPool<SHARD_CNT>> {
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

impl<const SHARD_CNT: usize> BufferPool for ThreeTierBufferPool<SHARD_CNT> {
    fn reserve_page<'a>(&'a self, page_type: PageType) -> Result<WriteGuard, OutOfSpaceError> {
        counter!(metric::Counter::ReservePage);
        let new_page_id = self
            .current_page_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pid = Pid::from(new_page_id);
        let backoff = Backoff::new();

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

        // First, try to insert to local memory if it has slots.
        if let Ok(mut wg) = self
            .local_buffer
            .get()
            .probe_entry_evict(|_ptr| -> Option<WriteGuard<'a>> { None }, fill_callback)
        {
            let ptr = wg.as_mut();
            self.page_table.insert(pid, PhysicalPage::LocalMemory(ptr));
            return Ok(wg);
        };

        // Second, insert to remote memory with replacement.
        loop {
            let rv = self
                .remote_buffer
                .get()
                .probe_entry_evict(|ptr| evict_to_disk_callback(ptr, self), fill_callback);
            match rv {
                Ok(mut fill_lock) => {
                    let ptr = fill_lock.as_mut();
                    self.page_table.insert(pid, PhysicalPage::TieredMemory(ptr));
                    return Ok(fill_lock);
                }
                Err(JasmineError::CacheEmpty) => {
                    panic!("Cache is empty.");
                }
                Err(_) => {
                    backoff.snooze();
                    continue;
                }
            }
        }
    }

    fn resolve_pid(&self, pid: crate::pid::Pid) -> *mut BaseNode {
        let backoff = Backoff::new();
        'outer: loop {
            let page = self.page_table.get(&pid).unwrap().value().clone();
            match page {
                PhysicalPage::LocalMemory(ptr) => {
                    counter!(metric::Counter::BtreeLocalHit);
                    unsafe {
                        self.local_buffer.mark_referenced(ptr as *const u8);
                    }
                    if unsafe { &*ptr }.page_type == PageType::Leaf {
                        counter!(metric::Counter::BtreeLeafLocalHit);
                    }
                    return ptr;
                }
                PhysicalPage::TieredMemory(ptr) => {
                    crate::utils::remote_delay();
                    match self.resolve_tiered_memory(&pid, ptr) {
                        Some(ptr) => {
                            counter!(metric::Counter::BtreeRemoteHit);
                            if unsafe { &*ptr }.page_type == PageType::Leaf {
                                counter!(metric::Counter::BtreeLeafRemoteHit);
                            }
                            return ptr;
                        }
                        None => {
                            backoff.snooze();
                            continue 'outer;
                        }
                    }
                }
                PhysicalPage::OnDisk(offset) => {
                    counter!(metric::Counter::BtreeDiskHit);
                    match self.resolve_on_disk(&pid, offset) {
                        Some(ptr) => {
                            if unsafe { &*ptr }.page_type == PageType::Leaf {
                                counter!(metric::Counter::BtreeLeafDiskHit);
                            }
                            return ptr;
                        }
                        None => {
                            backoff.snooze();
                            continue 'outer;
                        }
                    };
                }
                PhysicalPage::Inflight => {
                    backoff.snooze();
                    continue;
                }
            }
        }
    }

    fn free_page(&self, pid: crate::pid::Pid, mut wg: WriteGuard) {
        let expected_ptr = wg.as_mut() as *mut BaseNode;
        wg.mark_obsolete();
        std::mem::forget(wg);

        counter!(metric::Counter::FreePage);

        let old = self
            .page_table
            .get(&pid)
            .expect("The value must exists.")
            .value()
            .clone();
        // TODO: we need to delete this pid from the page table, to improve page table efficiency.
        // We can only delete this page in the epoch guard, which is not easy with current epoch guard.

        match old {
            PhysicalPage::LocalMemory(ptr) => unsafe {
                assert_eq!(ptr, expected_ptr);
                self.local_buffer.mark_empty(ptr as *const u8);
            },
            PhysicalPage::TieredMemory(ptr) => unsafe {
                assert_eq!(ptr, expected_ptr);
                self.remote_buffer.mark_empty(ptr as *const u8);
            },
            _ => {
                unreachable!()
            }
        }
    }

    unsafe fn bench_finished_loading_hint(&self) {
        self.direct_file.copy_to_real_disk();
    }
}

impl<const SHARD_CNT: usize> ThreeTierBufferPool<SHARD_CNT> {
    pub fn new(dram_buffer_byte: usize, remote_buffer_byte: usize, promote_remote: bool) -> Self {
        ThreeTierBufferPool {
            page_table: HashMap::new(),
            local_buffer: jasmine_db::ShardCache::new(
                dram_buffer_byte,
                std::alloc::Layout::from_size_align(PAGE_SIZE, PAGE_SIZE).unwrap(),
                douhua::MemType::DRAM,
            ),
            remote_buffer: jasmine_db::ShardCache::new(
                remote_buffer_byte,
                std::alloc::Layout::from_size_align(PAGE_SIZE, PAGE_SIZE).unwrap(),
                douhua::MemType::NUMA,
            ),
            direct_file: VFS::default(),
            current_page_id: AtomicUsize::new(1),
            promote_remote,
        }
    }

    fn resolve_tiered_memory(
        &self,
        pid: &Pid,
        page_on_tiered: *mut BaseNode,
    ) -> Option<*mut BaseNode> {
        if self.promote_remote
            && rand::thread_rng().gen_range(0..100) < crate::utils::get_promotion_rate()
        {
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
                        evict_to_remote_callback::<{ SHARD_CNT }>(
                            ptr,
                            evict_buffer,
                            &self.page_table,
                        )
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
                        } else {
                            unsafe {
                                self.remote_buffer.mark_empty(page_on_tiered as *const u8);
                            }
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
        } else {
            unsafe {
                self.remote_buffer
                    .mark_referenced(page_on_tiered as *const u8);
            }
            Some(page_on_tiered)
        }
    }

    fn resolve_on_disk(&self, pid: &Pid, offset: usize) -> Option<*mut BaseNode> {
        let page = PhysicalPage::OnDisk(offset);
        self.page_table
            .compare_exchange(pid, &page, PhysicalPage::Inflight)
            .ok()?;
        let fill_callback = |evict, ptr: *mut u8| {
            self.direct_file.read(offset * PAGE_SIZE, unsafe {
                std::slice::from_raw_parts_mut(ptr, PAGE_SIZE)
            });
            let node_ptr = ptr as *mut BaseNode;
            unsafe { &mut *node_ptr }
                .lock
                .fetch_and(!0b11, std::sync::atomic::Ordering::Relaxed); // reset the lock on loading.

            (evict, node_ptr)
        };

        let backoff = Backoff::new();

        'inner: loop {
            let replace_result = self
                .remote_buffer
                .get()
                .probe_entry_evict(|ptr| evict_to_disk_callback(ptr, self), fill_callback);
            match replace_result {
                Ok((evict_lock, ptr)) => {
                    std::mem::forget(evict_lock);
                    self.page_table
                        .compare_exchange(
                            pid,
                            &PhysicalPage::Inflight,
                            PhysicalPage::TieredMemory(ptr),
                        )
                        .expect("We should be the only one modifying the page now.");

                    if cfg!(debug_assertions) {
                        let node = unsafe { &*ptr };
                        assert!(!node.is_dirty);
                    }
                    counter!(metric::Counter::DiskToRemoteOk);

                    return Some(ptr);
                }
                Err(_e) => {
                    counter!(metric::Counter::DiskToRemoteFailed);
                    backoff.snooze();
                    continue 'inner;
                }
            }
        }
    }
}

/// Evict a page from local memory to remote memory.
pub(super) fn evict_to_remote_callback<const SHARD_CNT: usize>(
    ptr: *mut u8,
    dst: *mut BaseNode,
    page_table: &HashMap<Pid, PhysicalPage>,
) -> Option<()> {
    let evict_ptr = ptr as *mut BaseNode;
    let page_to_evict = match BaseNode::write_lock_new(evict_ptr) {
        Ok(wg) => wg,
        Err(TreeError::Obsolete) => {
            unreachable!("This is not allowed as the buffer must actively mark a page as empty if it is obsolete.");
        }
        Err(_e) => return None,
    };

    let pid = page_to_evict.as_ref().pid();
    page_table
        .compare_exchange(
            &pid,
            &PhysicalPage::LocalMemory(evict_ptr),
            PhysicalPage::Inflight,
        )
        .expect(
            "We should be the only one modifying the page now, because we are holding the lock.",
        );

    std::mem::forget(page_to_evict);

    unsafe {
        std::ptr::copy_nonoverlapping(evict_ptr as *const u8, dst as *mut u8, PAGE_SIZE);
    }
    Some(())
}

/// This function is only for tiered buffer,
/// meaning that only when the memory is in tiered buffer should it be evicted to the disk.
///
/// Is it possible to have multiple threads evicting the same page
/// Yes, but not in realistic settings where it's almost impossible to have probe pointer wrap around.
fn evict_to_disk_callback<const SHARD_CNT: usize>(
    ptr: *mut u8,
    pool: &ThreeTierBufferPool<SHARD_CNT>,
) -> Option<WriteGuard> {
    let old_ptr = ptr as *mut BaseNode;
    let mut old = match BaseNode::write_lock_new(old_ptr) {
        Ok(v) => v,
        Err(TreeError::Obsolete) => {
            unreachable!("This is no long allowed, as buffer pool should actively evict the pages from the pool.");
        }
        Err(_) => return None,
    };
    let old_page = PhysicalPage::TieredMemory(old_ptr);
    let new_page = PhysicalPage::OnDisk(old.as_ref().pid);

    let pid = old.as_ref().pid();

    if cfg!(debug_assertions) {
        let v = pool.page_table.get(&pid).unwrap().value().clone();
        assert_eq!(v, old_page);
    }
    if old.as_ref().is_dirty {
        old.as_mut().is_dirty = false;
        pool.direct_file
            .write(old.as_ref().pid * PAGE_SIZE, unsafe {
                std::slice::from_raw_parts(old_ptr as *const u8, PAGE_SIZE)
            });
    }
    pool.page_table
        .compare_exchange(&pid, &old_page, new_page)
        .expect("We should be the only one modifying the page now.");
    Some(old)
}
