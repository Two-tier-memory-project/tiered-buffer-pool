use crate::RecorderImpl;
use metric_utils::MetricEnum;
use serde::{ser::SerializeMap, Serialize, Serializer};

#[repr(u8)]
#[derive(Debug, Hash, Eq, PartialEq, MetricEnum)]
pub enum Counter {
    DbLocalHit = 0,
    DbRemoteHit = 1,
    DbDiskHit = 2,
    RemoteToLocalOk = 3,
    RemoteToLocalFailed = 4,
    DiskToRemoteOk = 5,
    DiskToRemoteFailed = 6,
    ReservePage = 7,
    FreePage = 8,
    BtreeInnerSplit = 9,
    BtreeLeafSplit = 10,
    BtreeNodeRemove = 11,
    BtreeNodeRemoveCaveat = 12,
    BtreeLeafRemoteHit = 13,
    BtreeLeafDiskHit = 14,
    BtreeLeafLocalHit = 15,
    BtreeLocalHit = 16,
    BtreeRemoteHit = 17,
    BtreeDiskHit = 18,
    TopEvicted = 19,
    MiddleEvicted = 20,
    ReserveInMemoryPage = 21,
    FreeInMemoryPage = 22,
    BtreeLeafSplitSuccess = 23,
    BtreeInnerSplitSuccess = 24,
    TwoTreeInsertUpper = 25,
    TwoTreeInsertLower = 26,
    BtreeLeafReserve = 27,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct CounterRecorder {
    pub(crate) counters: [u32; Counter::LENGTH],
}

impl CounterRecorder {
    pub(crate) fn increment(&mut self, event: Counter, amount: u32) {
        let counter = unsafe { self.counters.get_unchecked_mut(event as usize) };
        *counter += amount;
    }
}

impl Serialize for CounterRecorder {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_map(Some(Counter::LENGTH))?;
        for i in 0..self.counters.len() {
            let val = &self.counters[i];
            let key = Counter::from_num(i);
            state.serialize_key(&format!("{key:?}"))?;
            state.serialize_value(val)?;
        }
        state.end()
    }
}

impl RecorderImpl for CounterRecorder {
    fn reset(&mut self) {
        for i in self.counters.iter_mut() {
            *i = 0;
        }
    }
}

use auto_ops::impl_op_ex;

impl_op_ex!(+= |a: &mut CounterRecorder, b: &CounterRecorder| {
    for i in 0..Counter::LENGTH{
        a.counters[i] += b.counters[i];
    }
});

impl_op_ex!(+ |a: &CounterRecorder, b: &CounterRecorder| -> CounterRecorder {
    let mut c_a = a.clone();
    c_a += b;
    c_a
});
