use std::{cell::UnsafeCell, sync::atomic::Ordering};

use super::basic::BaseNode;
use crate::utils::TreeError;

#[derive(Debug)]
pub struct ReadGuard<'a> {
    version: u64,
    node: &'a UnsafeCell<BaseNode>,
}

impl<'a> ReadGuard<'a> {
    pub(crate) fn new(v: u64, node: &'a BaseNode) -> Self {
        Self {
            version: v,
            node: unsafe { &*(node as *const BaseNode as *const UnsafeCell<BaseNode>) }, // todo: the caller should pass UnsafeCell<BaseNode> instead
        }
    }

    pub(crate) fn check_version(&self) -> Result<u64, TreeError> {
        let v = self.as_ref().lock.load(Ordering::Acquire);

        if v == self.version {
            Ok(v)
        } else {
            Err(TreeError::VersionNotMatch(v))
        }
    }

    pub(crate) fn as_ref(&self) -> &BaseNode {
        unsafe { &*self.node.get() }
    }

    pub(crate) fn upgrade(self) -> Result<WriteGuard<'a>, (Self, TreeError)> {
        let new_version = self.version + 0b10;
        match self.as_ref().lock.compare_exchange_weak(
            self.version,
            new_version,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(WriteGuard {
                node: unsafe { &mut *self.node.get() },
            }),
            Err(v) => Err((self, TreeError::VersionNotMatch(v))),
        }
    }
}

#[derive(Debug)]
pub struct WriteGuard<'a> {
    pub(crate) node: &'a mut BaseNode,
}

impl<'a> WriteGuard<'a> {
    pub(crate) fn as_ref(&self) -> &BaseNode {
        self.node
    }

    pub(crate) fn as_mut(&mut self) -> &mut BaseNode {
        self.node
    }

    pub(crate) fn mark_obsolete(&mut self) {
        self.node.lock.fetch_add(0b01, Ordering::Release);
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        self.node.is_dirty = true;
        assert!(BaseNode::is_locked(self.node.lock.load(Ordering::Relaxed)));
        self.node.lock.fetch_add(0b10, Ordering::Release);
    }
}
