use metric::TlsRecorder;
use serde::Serialize;

#[derive(Serialize, Default, Clone)]
pub struct MicroBenchResult {
    throughput: usize,
    metrics: TlsRecorder,
}

impl MicroBenchResult {
    pub fn new(throughput: usize, metrics: TlsRecorder) -> Self {
        Self {
            throughput,
            metrics,
        }
    }
}

impl std::fmt::Display for MicroBenchResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Throughput: {} ops/sec", self.throughput)
    }
}

impl std::ops::Add for MicroBenchResult {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            throughput: self.throughput + other.throughput,
            metrics: self.metrics + other.metrics,
        }
    }
}

impl std::ops::AddAssign for MicroBenchResult {
    fn add_assign(&mut self, other: Self) {
        self.throughput += other.throughput;
        self.metrics += other.metrics;
    }
}

impl shumai::BenchResult for MicroBenchResult {
    fn short_value(&self) -> usize {
        self.throughput
    }

    fn normalize_time(self, dur: &std::time::Duration) -> Self {
        let dur = dur.as_secs_f64();
        let throughput = (self.throughput as f64) / dur;
        Self {
            throughput: throughput as usize,
            metrics: self.metrics,
        }
    }
}
