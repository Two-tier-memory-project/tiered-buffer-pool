use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Distribution {
    Uniform,
    SelfSimilar(f64),
    HotSpot(f64),
    Zipf(f64),
}

pub enum Workload {
    Read,
    NegativeRead,
    Update,
    Scan,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorkloadMix {
    read: u64,
    negative_read: u64,
    update: u64,
    scan: u64,
}

impl WorkloadMix {
    pub fn gen(&self, rng: &mut impl rand::Rng) -> Workload {
        debug_assert!(self.is_valid());
        let val = rng.gen_range(0..100);

        if val < self.read {
            Workload::Read
        } else if val < self.read + self.negative_read {
            Workload::NegativeRead
        } else if val < self.read + self.negative_read + self.update {
            Workload::Update
        } else {
            Workload::Scan
        }
    }

    pub fn is_valid(&self) -> bool {
        self.read + self.update + self.scan == 100
    }
}
