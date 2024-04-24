use crate::{
    btree::{basic::BaseNode, lock::WriteGuard, PageType},
    pid::Pid,
};
pub(crate) mod in_memory;
mod three_tier_bp;
mod two_tier_bp;
mod two_tier_memory;
mod two_tier_memory_blind;
mod utils;
pub(crate) mod vfs;

pub use in_memory::InMemory;
pub use three_tier_bp::ThreeTierBufferPool;
pub use two_tier_bp::TwoTierBufferPool;
pub use two_tier_memory::TwoTierMemory;
pub use two_tier_memory_blind::TwoTierMemoryBlind;

#[derive(Debug, Clone)]
pub struct OutOfSpaceError {
    page_type: PageType,
}

impl OutOfSpaceError {
    fn new(page_type: PageType) -> Self {
        Self { page_type }
    }
}

impl std::error::Error for OutOfSpaceError {}
impl std::fmt::Display for OutOfSpaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Out of space for page type: {:?}", self.page_type)
    }
}

pub trait BufferPool: Send + Sync {
    fn reserve_page(&self, page_type: PageType) -> Result<WriteGuard, OutOfSpaceError>;

    fn free_page(&self, pid: Pid, wg: WriteGuard);

    /// The interface is deliberately designed such that the caller must clone a pid instead of using the snapshot.
    fn resolve_pid(&self, pid: Pid) -> *mut BaseNode;

    /// Hint the buffer pool that the bench has finished loading.
    /// It might be helpful to tweek some parameters.
    /// Currently only used for the FastLoadFile
    ///
    /// # Safety
    /// Only one thread should call this function.
    unsafe fn bench_finished_loading_hint(&self) {}
}

/// The highest bit shows whether the page is in memory or on disk.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) enum PhysicalPage {
    LocalMemory(*mut BaseNode),
    TieredMemory(*mut BaseNode),
    OnDisk(usize), // when page is on disk, the pid is the page offset.
    Inflight,
}

unsafe impl Sync for PhysicalPage {}
unsafe impl Send for PhysicalPage {}

impl From<usize> for PhysicalPage {
    fn from(id: usize) -> Self {
        if (id >> 62) == 2 {
            PhysicalPage::OnDisk(id & 0x7fff_ffff_ffff_ffff)
        } else if (id >> 62) == 1 {
            PhysicalPage::Inflight
        } else if (id >> 62) == 3 {
            let id = id & !0xc000_0000_0000_0000;
            PhysicalPage::TieredMemory(id as *mut BaseNode)
        } else {
            PhysicalPage::LocalMemory(id as *mut BaseNode)
        }
    }
}

impl From<PhysicalPage> for usize {
    fn from(page: PhysicalPage) -> Self {
        match page {
            PhysicalPage::LocalMemory(ptr) => ptr as usize,
            PhysicalPage::OnDisk(id) => id | 0x8000_0000_0000_0000,
            PhysicalPage::TieredMemory(ptr) => (ptr as usize) | 0xc000_0000_0000_0000,
            PhysicalPage::Inflight => 0x4000_0000_0000_0000,
        }
    }
}
