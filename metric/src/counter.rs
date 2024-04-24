use crate::RecorderImpl;
use metric_utils::MetricEnum;
use serde::{ser::SerializeMap, Serialize, Serializer};

#[repr(u8)]
#[derive(Debug, Hash, Eq, PartialEq, MetricEnum)]
pub enum Counter {
    LocalMemHit = 0,
    RemoteMemHit = 1,
    DiskHit = 2,
    RemoteToLocalOk = 3,
    RemoteToLocalFailed = 4,
    DiskToRemoteOk = 5,
    DiskToRemoteFailed = 6,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct CounterRecorder {
    pub(crate) counters: [u64; Counter::LENGTH],
}

impl CounterRecorder {
    pub(crate) fn increment(&mut self, event: Counter, amount: u64) {
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
            state.serialize_key(&format!("{:?}", key))?;
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
