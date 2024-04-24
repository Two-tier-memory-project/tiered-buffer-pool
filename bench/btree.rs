use std::sync::Arc;

use btree_olc::buffer_pool::ThreeTierBufferPool;
use rand::{rngs::SmallRng, thread_rng, Rng, SeedableRng};
use shumai::{config, ShumaiBench};
use two_tree_storage::bench::{config::Workload, murmur64, Sampler, SmallTuple};
use two_tree_storage::DBTuple;

#[config(path = "bench/benchmark.toml")]
pub struct BasicConfig {
    pub name: String,
    pub threads: Vec<usize>,
    pub time: usize,
    pub distribution: two_tree_storage::bench::config::Distribution,
    pub record_cnt: usize,
    pub workload_mix: two_tree_storage::bench::config::WorkloadMix,
    #[matrix]
    pub dram_size_mb: usize,
    #[matrix]
    pub remote_size_mb: usize,
}

struct TestDB {
    db: btree_olc::btree::Btree<usize, SmallTuple, ThreeTierBufferPool<8>>,
    config: BasicConfig,
}

impl TestDB {
    fn new(config: BasicConfig) -> TestDB {
        let pool = Arc::new(ThreeTierBufferPool::new(
            config.dram_size_mb * 1024 * 1024,
            config.remote_size_mb * 1024 * 1024,
            true,
        ));
        TestDB {
            db: btree_olc::btree::Btree::new(pool),
            config,
        }
    }
}

const LOAD_THREAD: usize = 8;

impl ShumaiBench for TestDB {
    type Result = usize;
    type Config = BasicConfig;

    fn run(&self, context: shumai::Context<Self::Config>) -> Self::Result {
        let mut small_rng = SmallRng::from_entropy();
        let sampler = Sampler::from(&self.config.distribution, 0..self.config.record_cnt);

        let guard = self.db.pin();
        context.wait_for_start();

        let mut op_cnt = 0;
        while context.is_running() {
            let op = self.config.workload_mix.gen(&mut small_rng);
            match op {
                Workload::Read => {
                    let key = sampler.sample(&mut small_rng);
                    let key = murmur64(key);
                    if thread_rng().gen_range(0..100) < 15 {}
                    match self.db.get(&key, &guard) {
                        Some(val) => {
                            assert_eq!(val, SmallTuple::new(key));
                        }
                        None => {
                            panic!("Value must exists, key: {}", key);
                        }
                    }
                }
                Workload::NegativeRead => {}
                Workload::Update => {
                    unimplemented!();
                }
                Workload::Scan => {
                    todo!()
                }
            }
            op_cnt += 1;
        }
        op_cnt
    }

    fn load(&mut self) -> Option<serde_json::Value> {
        let record_per_thread = self.config.record_cnt / LOAD_THREAD;
        std::thread::scope(|s| {
            let mut handlers = Vec::new();
            for t in 0..LOAD_THREAD {
                let db = &self.db;
                let h = s.spawn(move || {
                    let guard = crossbeam_epoch::pin();
                    for i in 0..record_per_thread {
                        let key = i + t * record_per_thread;
                        let key = murmur64(key);
                        let val = SmallTuple::new(key);
                        db.insert(val.primary_key(), val, &guard).unwrap();
                    }
                });
                handlers.push(h);
            }
            for h in handlers {
                h.join().unwrap();
            }
        });

        None
    }

    fn cleanup(&mut self) -> Option<serde_json::Value> {
        None
    }
}

fn main() {
    let config = BasicConfig::load().expect("Failed to parse config!");
    for c in config {
        let mut sut = TestDB::new(c.clone());
        let results = shumai::run(&mut sut, &c, 5);
        results.write_json().expect("Failed to write results!");
    }
}
