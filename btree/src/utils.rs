#[derive(Debug)]
pub(crate) enum TreeError {
    VersionNotMatch(u64),
    Locked(u64),
    Obsolete,
    NeedRestart,
    OutOfSpace(OutOfSpaceError),
}

use core::cell::Cell;
use core::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::buffer_pool::OutOfSpaceError;

const SPIN_LIMIT: u32 = 6;
const YIELD_LIMIT: u32 = 10;

/// Backoff implementation from the Crossbeam, added shuttle instrumentation
pub(crate) struct Backoff {
    step: Cell<u32>,
}

impl Backoff {
    #[inline]
    pub(crate) fn new() -> Self {
        Backoff { step: Cell::new(0) }
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn reset(&self) {
        self.step.set(0);
    }

    #[inline]
    pub(crate) fn spin(&self) {
        for _ in 0..1 << self.step.get().min(SPIN_LIMIT) {
            std::hint::spin_loop();
        }

        if self.step.get() <= SPIN_LIMIT {
            self.step.set(self.step.get() + 1);
        }

        #[cfg(feature = "shuttle")]
        shuttle::thread::yield_now();
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn snooze(&self) {
        if self.step.get() <= SPIN_LIMIT {
            for _ in 0..1 << self.step.get() {
                std::hint::spin_loop();
            }
        } else {
            #[cfg(feature = "shuttle")]
            shuttle::thread::yield_now();

            #[cfg(not(feature = "shuttle"))]
            ::std::thread::yield_now();
        }

        if self.step.get() <= YIELD_LIMIT {
            self.step.set(self.step.get() + 1);
        }
    }

    #[inline]
    pub(crate) fn is_completed(&self) -> bool {
        self.step.get() > YIELD_LIMIT
    }
}

impl fmt::Debug for Backoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backoff")
            .field("step", &self.step)
            .field("is_completed", &self.is_completed())
            .finish()
    }
}

impl Default for Backoff {
    fn default() -> Backoff {
        Backoff::new()
    }
}

static DELAY_LOOP_CNT: AtomicUsize = AtomicUsize::new(0);

const SPIN_LOOP_DELAY: usize = 175;

pub fn set_remote_memory_delay(delay: Duration) {
    let nanos = delay.as_nanos() as usize;
    let delay_cnt = nanos / SPIN_LOOP_DELAY;
    DELAY_LOOP_CNT.store(delay_cnt, Ordering::Relaxed);
}

#[cfg(not(test))]
pub(crate) mod config {
    use std::sync::atomic::AtomicUsize;

    pub static PROMOTION_RATE: AtomicUsize = AtomicUsize::new(5);
}

#[cfg(test)]
pub(crate) mod config {
    use std::sync::atomic::AtomicUsize;

    pub static PROMOTION_RATE: AtomicUsize = AtomicUsize::new(100);
}

pub fn set_promotion_rate(rate: usize) {
    config::PROMOTION_RATE.store(rate, Ordering::Relaxed);
}

pub fn get_promotion_rate() -> usize {
    config::PROMOTION_RATE.load(Ordering::Relaxed)
}

#[inline]
#[allow(unused_variables)]
pub fn remote_delay() {
    #[cfg(feature = "add_delay")]
    {
        for _ in 0..DELAY_LOOP_CNT.load(Ordering::Relaxed) {
            std::hint::spin_loop();
        }
    }
}
