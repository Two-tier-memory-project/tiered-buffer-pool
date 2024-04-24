use crate::pid::Pid;

use super::{basic::BaseNode, lock::WriteGuard, PAGE_SIZE};

pub const fn btree_inner_entries<Key>() -> usize {
    (PAGE_SIZE - std::mem::size_of::<BaseNode>())
        / (std::mem::size_of::<Key>() + std::mem::size_of::<Pid>())
}

#[repr(C)]
pub(crate) struct BtreeInner<Key: PartialOrd + Clone>
where
    [(); btree_inner_entries::<Key>()]:,
{
    pub(crate) base: BaseNode,
    pub(crate) keys: [Key; btree_inner_entries::<Key>()],
    pub(crate) children: [Pid; btree_inner_entries::<Key>()],
}

impl<Key: PartialOrd + Clone> BtreeInner<Key>
where
    [(); btree_inner_entries::<Key>()]:,
{
    pub(crate) fn is_full(&self) -> bool {
        self.base.count == (btree_inner_entries::<Key>() as u16 - 1)
    }

    pub(crate) fn lower_bound(&self, key: &Key) -> usize {
        let mut lower = 0;
        let mut upper = self.base.count as usize;

        loop {
            let mid = (upper + lower) / 2;
            if *key < self.keys[mid] {
                upper = mid;
            } else if *key > self.keys[mid] {
                lower = mid + 1;
            } else {
                return mid;
            }
            if lower >= upper {
                return lower;
            }
        }
    }

    pub(crate) fn remove(&mut self, child_pos: usize) -> (Key, Pid) {
        assert!(child_pos <= self.base.count as usize);
        assert!(self.base.count > 0);
        let key_pos = if child_pos == 0 { 0 } else { child_pos - 1 };
        let val = unsafe { std::ptr::read(self.children.as_ptr().add(child_pos)) };
        let key = unsafe { std::ptr::read(self.keys.as_ptr().add(key_pos)) };
        unsafe {
            std::ptr::copy(
                self.keys.as_mut_ptr().add(key_pos + 1),
                self.keys.as_mut_ptr().add(key_pos),
                self.base.count as usize - key_pos - 1,
            );
            std::ptr::copy(
                self.children.as_mut_ptr().add(child_pos + 1),
                self.children.as_mut_ptr().add(child_pos),
                self.base.count as usize - child_pos, // the children has count+1
            );
        }
        self.base.count -= 1;
        (key, val)
    }

    pub(crate) fn split<'a>(&mut self, mut new_node: WriteGuard<'a>) -> (WriteGuard<'a>, Key) {
        let new_node_inner = new_node.as_mut().as_inner_mut();
        new_node_inner.base.count = self.base.count / 2;
        self.base.count = self.base.count - new_node_inner.base.count - 1;

        unsafe {
            std::ptr::copy_nonoverlapping(
                self.keys.as_ptr().add(self.base.count as usize + 1),
                new_node_inner.keys.as_mut_ptr(),
                new_node_inner.base.count as usize + 1,
            );
            std::ptr::copy_nonoverlapping(
                self.children.as_ptr().add(self.base.count as usize + 1),
                new_node_inner.children.as_mut_ptr(),
                new_node_inner.base.count as usize + 1,
            )
        }
        (new_node, self.keys[self.base.count as usize].clone())
    }

    pub(crate) fn insert(&mut self, key: Key, child: Pid) {
        assert!(self.base.count < btree_inner_entries::<Key>() as u16 - 1);
        let pos = self.lower_bound(&key);
        unsafe {
            std::ptr::copy(
                self.keys.as_mut_ptr().add(pos),
                self.keys.as_mut_ptr().add(pos + 1),
                self.base.count as usize - pos + 1,
            );
            std::ptr::copy(
                self.children.as_mut_ptr().add(pos),
                self.children.as_mut_ptr().add(pos + 1),
                self.base.count as usize - pos + 1,
            );
        }
        self.keys[pos] = key;
        self.children[pos] = child;
        unsafe {
            std::ptr::swap(
                self.children.as_mut_ptr().add(pos),
                self.children.as_mut_ptr().add(pos + 1),
            );
        }
        self.base.count += 1;
    }
}
