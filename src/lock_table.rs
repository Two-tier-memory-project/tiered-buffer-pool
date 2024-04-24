use dashmap::mapref::entry::Entry;
use fxhash::FxBuildHasher;

#[derive(Debug, Clone, PartialEq)]
pub enum LockState {
    Read(u32),
    Write,
}

impl From<usize> for LockState {
    fn from(lock: usize) -> Self {
        if lock == 1 {
            LockState::Write
        } else {
            let read_cnt = lock >> 1;
            LockState::Read(read_cnt as u32)
        }
    }
}

impl From<LockState> for usize {
    fn from(lock: LockState) -> Self {
        match lock {
            LockState::Read(read_cnt) => (read_cnt << 1) as usize,
            LockState::Write => 1,
        }
    }
}

pub trait SharedLocked {
    fn lock_key(&self) -> &usize;
}

impl<'a> SharedLocked for ReadGuard<'a> {
    fn lock_key(&self) -> &usize {
        self.lock_key()
    }
}

impl<'a> SharedLocked for WriteGuard<'a> {
    fn lock_key(&self) -> &usize {
        self.lock_key()
    }
}

/// The lock guard for a record.
/// Unlock the record on drop (traversing the index to update the leaf node).
pub struct ReadGuard<'a> {
    primary_key: usize,
    lock_table: &'a LockTable,
}

impl<'a> ReadGuard<'a> {
    fn new(primary_key: usize, lock_table: &'a LockTable) -> Self {
        ReadGuard {
            primary_key,
            lock_table,
        }
    }

    pub fn lock_key(&self) -> &usize {
        &self.primary_key
    }

    pub fn upgrade(self) -> Result<WriteGuard<'a>, ReadGuard<'a>> {
        match self.lock_table.inner.entry(self.primary_key) {
            Entry::Occupied(mut e) => {
                let old = e.get();
                if let LockState::Read(read_cnt) = old {
                    if *read_cnt == 1 {
                        *e.get_mut() = LockState::Write;
                        let (pk, lt) = (self.primary_key, self.lock_table);
                        std::mem::forget(self);
                        Ok(WriteGuard::new(pk, lt))
                    } else {
                        Err(self)
                    }
                } else {
                    unreachable!("ReadGuard::upgrade() called on a write lock");
                }
            }
            Entry::Vacant(_e) => {
                unreachable!("A read lock must exist before upgrading");
            }
        }
    }
}

impl<'a> Drop for ReadGuard<'a> {
    fn drop(&mut self) {
        match self.lock_table.inner.entry(self.primary_key) {
            Entry::Occupied(mut e) => {
                let old = e.get();
                if let LockState::Read(read_cnt) = old {
                    if *read_cnt == 1 {
                        e.remove();
                    } else {
                        *e.get_mut() = LockState::Read(read_cnt - 1);
                    }
                } else {
                    unreachable!("ReadGuard::drop() called on a write lock");
                }
            }
            Entry::Vacant(_e) => {
                unreachable!("A read lock must exist before dropping");
            }
        }
    }
}

/// The lock guard for a record.
/// Unlock the record on drop (traversing the index to update the leaf node).
pub struct WriteGuard<'a> {
    primary_key: usize,
    lock_table: &'a LockTable,
}

impl<'a> WriteGuard<'a> {
    pub(crate) fn new(primary_key: usize, lock_table: &'a LockTable) -> Self {
        WriteGuard {
            primary_key,
            lock_table,
        }
    }

    pub fn lock_key(&self) -> &usize {
        &self.primary_key
    }

    /// Downgrade the write lock to a read lock, this can't fail.
    pub fn downgrade(self) -> ReadGuard<'a> {
        let mut val = self
            .lock_table
            .inner
            .get_mut(&self.primary_key)
            .expect("Must exist");
        *val = LockState::Read(1);

        let rg = ReadGuard::new(self.primary_key, self.lock_table);
        std::mem::forget(self);
        rg
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        let old = self
            .lock_table
            .inner
            .remove(&self.primary_key)
            .expect("lock must exist");
        debug_assert!(matches!(old.1, LockState::Write));
    }
}

pub(crate) struct LockTable {
    inner: dashmap::DashMap<usize, LockState, FxBuildHasher>,
}

impl LockTable {
    pub(crate) fn new() -> Self {
        let s = fxhash::FxBuildHasher::default();
        Self {
            inner: dashmap::DashMap::with_capacity_and_hasher_and_shard_amount(128, s, 32),
        }
    }

    #[must_use]
    pub(crate) fn write_lock(&self, key: usize) -> Option<WriteGuard> {
        match self.inner.entry(key) {
            Entry::Occupied(_e) => {
                #[cfg(debug_assertions)]
                {
                    let old = _e.get();
                    if let LockState::Read(cnt) = old {
                        debug_assert!(*cnt > 0);
                    }
                }
                None
            }
            Entry::Vacant(e) => {
                e.insert(LockState::Write);
                Some(WriteGuard::new(key, self))
            }
        }
    }

    #[must_use]
    pub(crate) fn read_lock(&self, key: &usize) -> Option<ReadGuard> {
        match self.inner.entry(*key) {
            Entry::Occupied(mut e) => {
                let old = e.get();
                match old {
                    LockState::Read(cnt) => {
                        *e.get_mut() = LockState::Read(cnt + 1);
                        Some(ReadGuard::new(*key, self))
                    }
                    LockState::Write => None,
                }
            }
            Entry::Vacant(e) => {
                e.insert(LockState::Read(1));
                Some(ReadGuard::new(*key, self))
            }
        }
    }
}
