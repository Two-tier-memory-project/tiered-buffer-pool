/// Pid is page id, each page has a unique page id.
///

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Pid {
    pub(crate) id: usize,
}

impl From<usize> for Pid {
    fn from(id: usize) -> Self {
        debug_assert!(id != 0);
        Pid { id }
    }
}

impl From<Pid> for usize {
    fn from(pid: Pid) -> Self {
        pid.id
    }
}

impl Pid {
    pub(crate) fn from_null() -> Self {
        Self { id: 0 }
    }

    pub(crate) fn is_null(&self) -> bool {
        self.id == 0
    }
}
