#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicU64, Ordering};

use douhua::MemType;
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicU64, Ordering};

use super::{
    inner::{btree_inner_entries, BtreeInner},
    leaf::{btree_leaf_entries, BtreeLeaf},
    lock::{ReadGuard, WriteGuard},
    PageType,
};
use crate::{pid::Pid, utils::TreeError};

#[derive(Debug)]
#[repr(C)]
pub struct BaseNode {
    pub(crate) lock: AtomicU64,
    pub(crate) pid: usize,
    pub(crate) mem_type: MemType,
    pub(crate) is_dirty: bool,
    pub(crate) page_type: PageType,

    // The count for leaf is the number of entries, for inner is the number of keys.
    // this means that the number of inner children is count + 1.
    pub(crate) count: u16,
}

impl BaseNode {
    pub(crate) fn new(disk_id: usize, page_type: PageType, mem_type: MemType) -> Self {
        BaseNode {
            lock: AtomicU64::new(0),
            pid: disk_id,
            mem_type,
            is_dirty: false,
            page_type,
            count: 0,
        }
    }

    pub(crate) fn is_locked(version: u64) -> bool {
        (version & 0b10) == 0b10
    }

    /// # Safety
    /// You should not use this function in common case, rely on the drop guard instead.
    /// This function is only used when the RAII style is not feasible.
    ///
    /// You should also make sure the original drop guard is forgotten.
    pub(crate) unsafe fn unlock(&self) {
        let old_v = self.lock.load(Ordering::Acquire);
        assert!(Self::is_locked(old_v));
        self.lock.store(old_v + 0b10, Ordering::Release);
    }

    pub(crate) fn is_obsolete(version: u64) -> bool {
        (version & 1) == 1
    }

    pub(crate) fn read_lock(&self) -> Result<ReadGuard, TreeError> {
        let version = self.lock.load(Ordering::Acquire);
        if Self::is_locked(version) {
            return Err(TreeError::Locked(version));
        }
        if Self::is_obsolete(version) {
            return Err(TreeError::Obsolete);
        }
        return Ok(ReadGuard::new(version, self));
    }

    pub(crate) fn write_lock_new<'a>(
        base_node: *mut BaseNode,
    ) -> Result<WriteGuard<'a>, TreeError> {
        let base_node_ref = unsafe { &*base_node };
        let version = base_node_ref.lock.load(Ordering::Acquire);
        if Self::is_locked(version) || Self::is_obsolete(version) {
            return Err(TreeError::Locked(version));
        }
        let new_version = version + 0b10;
        match base_node_ref.lock.compare_exchange_weak(
            version,
            new_version,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(WriteGuard {
                node: unsafe { &mut *base_node },
            }),
            Err(v) => Err(TreeError::VersionNotMatch(v)),
        }
    }

    pub(crate) fn as_inner<K>(&self) -> &BtreeInner<K>
    where
        K: PartialOrd + Clone,
        [(); btree_inner_entries::<K>()]:,
    {
        assert!(self.page_type == PageType::Inner);
        unsafe { &*(self as *const BaseNode as *const BtreeInner<K>) }
    }

    pub(crate) fn as_inner_mut<K>(&mut self) -> &mut BtreeInner<K>
    where
        K: PartialOrd + Clone,
        [(); btree_inner_entries::<K>()]:,
    {
        assert!(self.page_type == PageType::Inner);
        unsafe { &mut *(self as *mut BaseNode as *mut BtreeInner<K>) }
    }

    pub(crate) fn as_leaf<K, V>(&self) -> &BtreeLeaf<K, V>
    where
        K: PartialOrd + Clone,
        V: Clone,
        [(); btree_leaf_entries::<K, V>()]:,
    {
        assert!(self.page_type == PageType::Leaf);
        unsafe { &*(self as *const BaseNode as *const BtreeLeaf<K, V>) }
    }

    pub(crate) fn as_leaf_mut<K, V>(&mut self) -> &mut BtreeLeaf<K, V>
    where
        K: PartialOrd + Clone,
        V: Clone,
        [(); btree_leaf_entries::<K, V>()]:,
    {
        assert!(self.page_type == PageType::Leaf);
        unsafe { &mut *(self as *mut BaseNode as *mut BtreeLeaf<K, V>) }
    }

    pub(crate) fn pid(&self) -> Pid {
        Pid::from(self.pid)
    }
}
