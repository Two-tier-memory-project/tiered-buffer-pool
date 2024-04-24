#![allow(incomplete_features)]
#![feature(generic_const_exprs)]
#![feature(slice_ptr_get)]

pub mod utils;

#[cfg(test)]
mod tests;

pub mod btree;
pub mod buffer_pool;
mod pid;

const PAGE_SIZE: usize = 4096;
// const PAGE_ALIGN: usize = PAGE_SIZE - 1;

#[allow(dead_code)]
fn yield_if_shuttle() {
    #[cfg(feature = "shuttle")]
    shuttle::thread::yield_now();
}

use std::fmt;

#[derive(Debug, Clone)]
pub struct InsertOutOfSpaceError<V> {
    pub not_inserted: V,
}

impl<V> InsertOutOfSpaceError<V> {
    fn new(value: V) -> Self {
        Self {
            not_inserted: value,
        }
    }
}

impl<V: fmt::Debug> std::error::Error for InsertOutOfSpaceError<V> {}
impl<V: fmt::Debug> std::fmt::Display for InsertOutOfSpaceError<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Out of space for page type: {:?}", self.not_inserted)
    }
}
