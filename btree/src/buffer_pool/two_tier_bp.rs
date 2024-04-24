use std::sync::atomic::AtomicUsize;

use douhua::MemType;
use metric::counter;

use crate::{
    btree::{basic::BaseNode, lock::WriteGuard, PageType},
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

pub struct TwoTierBufferPool<const SHARD_CNT: usize> {
    pub(super) page_table: HashMap<Pid, PhysicalPage>,
    mem_buffer: jasmine_db::ShardCache<{ SHARD_CNT }>,
    direct_file: vfs::FastLoadFile,
    current_page_id: AtomicUsize,
    mem_type: MemType,
    delay_leaf_only: bool,
}

impl<const SHARD_CNT: usize> BufferPool for TwoTierBufferPool<SHARD_CNT> {
    fn free_page(&self, pid: Pid, mut wg: crate::btree::lock::WriteGuard) {
        counter!(metric::Counter::FreePage);
        let expected_ptr = wg.as_mut() as *mut BaseNode;
        wg.mark_obsolete();
        std::mem::forget(wg);

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
                self.mem_buffer.mark_empty(ptr as *const u8);
            },
            _ => {
                unreachable!()
            }
        }
    }

    unsafe fn bench_finished_loading_hint(&self) {
        self.direct_file.copy_to_real_disk();
    }

    fn reserve_page<'a>(
        &'a self,
        page_type: crate::btree::PageType,
    ) -> Result<WriteGuard<'_>, OutOfSpaceError> {
        let new_page_id = self
            .current_page_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pid = Pid::from(new_page_id);
        let backoff = Backoff::new();
        counter!(metric::Counter::ReservePage);

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
                    let base = BaseNode::new(new_page_id, page_type.clone(), self.mem_type);
                    unsafe {
                        std::ptr::write(ptr, base);
                    }
                    BaseNode::write_lock_new(ptr).expect("No one should content with us.")
                }
            }
        };
        loop {
            let rv = self
                .mem_buffer
                .get()
                .probe_entry_evict(|ptr| evict_to_disk_callback(ptr, self), fill_callback);
            match rv {
                Ok(mut wg) => {
                    let ptr = wg.as_mut();
                    self.page_table.insert(pid, PhysicalPage::LocalMemory(ptr));
                    return Ok(wg);
                }
                Err(_e) => {
                    backoff.spin();
                    continue;
                }
            }
        }
    }

    fn resolve_pid(&self, pid: crate::pid::Pid) -> *mut BaseNode {
        let backoff = Backoff::new();
        loop {
            let page = self.page_table.get(&pid).unwrap().value().clone();
            match page {
                PhysicalPage::LocalMemory(ptr) => unsafe {
                    if self.mem_type == MemType::NUMA {
                        counter!(metric::Counter::BtreeRemoteHit);
                        if { &*ptr }.page_type == PageType::Leaf {
                            counter!(metric::Counter::BtreeLeafRemoteHit);
                        }
                        #[allow(clippy::if_same_then_else)]
                        if self.delay_leaf_only && { &*ptr }.page_type == PageType::Leaf {
                            crate::utils::remote_delay();
                        } else {
                            crate::utils::remote_delay();
                        }
                    } else {
                        if { &*ptr }.page_type == PageType::Leaf {
                            counter!(metric::Counter::BtreeLeafLocalHit);
                        }
                        counter!(metric::Counter::BtreeLocalHit);
                    }
                    self.mem_buffer.mark_referenced(ptr as *const u8);
                    return ptr;
                },
                PhysicalPage::TieredMemory(_) => {
                    unreachable!("TieredMemory is not supported here.")
                }
                PhysicalPage::OnDisk(offset) => match self.resolve_on_disk(&pid, offset) {
                    Some(ptr) => {
                        counter!(metric::Counter::BtreeDiskHit);
                        if unsafe { &*ptr }.page_type == PageType::Leaf {
                            counter!(metric::Counter::BtreeLeafDiskHit);
                        }
                        return ptr;
                    }
                    None => {
                        backoff.snooze();
                        continue;
                    }
                },
                PhysicalPage::Inflight => {
                    backoff.snooze();
                    continue;
                }
            }
        }
    }
}

impl<const SHARD_CNT: usize> TwoTierBufferPool<SHARD_CNT> {
    pub fn new(memory_size_byte: usize, mem_type: douhua::MemType, delay_leaf_only: bool) -> Self {
        Self {
            page_table: HashMap::new(),
            mem_buffer: jasmine_db::ShardCache::new(
                memory_size_byte,
                std::alloc::Layout::from_size_align(PAGE_SIZE, PAGE_SIZE).unwrap(),
                mem_type,
            ),
            direct_file: VFS::default(),
            current_page_id: AtomicUsize::new(1), // page id starts from 1.
            mem_type,
            delay_leaf_only,
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
            let replace_result = self.mem_buffer.get().probe_entry_evict(
                |ptr| {
                    if self.mem_type == MemType::NUMA {
                        crate::utils::remote_delay();
                    }
                    evict_to_disk_callback(ptr, self)
                },
                fill_callback,
            );
            match replace_result {
                Ok((evict_lock, ptr)) => {
                    std::mem::forget(evict_lock);
                    self.page_table
                        .compare_exchange(
                            pid,
                            &PhysicalPage::Inflight,
                            PhysicalPage::LocalMemory(ptr),
                        )
                        .expect("We should be the only one modifying the page now.");

                    if cfg!(debug_assertions) {
                        let node = unsafe { &*ptr };
                        assert!(!node.is_dirty);
                    }

                    if self.mem_type == MemType::NUMA {
                        counter!(metric::Counter::DiskToRemoteOk);
                    }
                    return Some(ptr);
                }
                Err(_e) => {
                    if self.mem_type == MemType::NUMA {
                        counter!(metric::Counter::DiskToRemoteFailed);
                    }
                    backoff.snooze();
                    continue 'inner;
                }
            }
        }
    }
}

fn evict_to_disk_callback<const SHARD_CNT: usize>(
    ptr: *mut u8,
    pool: &TwoTierBufferPool<SHARD_CNT>,
) -> Option<WriteGuard> {
    let old_ptr = ptr as *mut BaseNode;
    let mut old = match BaseNode::write_lock_new(old_ptr) {
        Ok(v) => v,
        Err(TreeError::Obsolete) => {
            unreachable!("This is no long allowed, as buffer pool should actively evict the pages from the pool.");
        }
        Err(_) => return None,
    };
    let old_page = PhysicalPage::LocalMemory(old_ptr);
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
