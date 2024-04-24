#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

use std::time::Duration;

use btree_olc::utils;
use rand::{rngs::SmallRng, SeedableRng};
use serde::{Deserialize, Serialize};
use shumai::{config, ShumaiBench};
use two_tree_storage::bench::{
    config::Workload, murmur64, results::MicroBenchResult, Sampler, SmallTuple,
};
use two_tree_storage::{
    BufferPoolDB, DBTuple, QueryError, TableInterface, ThreeTierOneTree, ThreeTierOneTreePool,
    ThreeTreeBtree, ThreeTreePool, TwoTierOneTree, TwoTierOneTreePool, TwoTreeLowerBtree,
    TwoTreeLowerBufferPool, TwoTreeUpperBlindBtree, TwoTreeUpperBlindBufferPool, TwoTreeUpperBtree,
    TwoTreeUpperBufferPool,
};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[config(path = "bench/benchmark.toml")]
pub struct DBConfig {
    pub name: String,
    pub threads: Vec<usize>,
    pub time: usize,
    #[matrix]
    pub distribution: two_tree_storage::bench::config::Distribution,
    pub record_cnt: usize,
    pub workload_mix: two_tree_storage::bench::config::WorkloadMix,
    #[matrix]
    pub sut: SystemType,
    #[matrix]
    pub dram_size_mb: usize,
    #[matrix]
    pub remote_size_mb: usize,
    #[matrix]
    pub delay_nanos: usize,
    #[matrix]
    pub promotion_rate: Option<usize>,
    #[matrix]
    pub evict_batch: Option<usize>,
    pub repeat: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemType {
    OneTree,
    TwoTreeLower,
    TwoTreeUpper,
    TwoTreeUpperBlind,
    ThreeTree,
    TwoTierOneTree,
}

const LOAD_THREAD: usize = 10;

struct TestDB<T: TableInterface<SmallTuple>> {
    db: T,
    config: DBConfig,
}

const MB: usize = 1024 * 1024;

impl<T: TableInterface<SmallTuple>> TestDB<T> {
    fn new(config: DBConfig, db: T) -> Self {
        TestDB { db, config }
    }

    #[allow(dead_code)]
    fn sanity_check(&self) {
        // Let's do a sanity check here, make sure we can read every value:
        let guard = crossbeam_epoch::pin();

        for i in 0..self.config.record_cnt {
            let key = murmur64(i);
            let lock = self.db.lock(&key).unwrap();
            match self.db.get(&key, &lock, &guard) {
                Ok(val) => {
                    assert_eq!(key, val.primary_key());
                }
                Err(e) => {
                    panic!("Value must exists, key: {}, got error: {:?}", key, e);
                }
            }
        }
    }
}

impl<T: TableInterface<SmallTuple>> ShumaiBench for TestDB<T> {
    type Result = MicroBenchResult;
    type Config = DBConfig;

    fn run(&self, context: shumai::Context<Self::Config>) -> Self::Result {
        metric::get_tls_recorder().reset();
        let mut small_rng = SmallRng::from_entropy();
        let sampler = Sampler::from(&self.config.distribution, 0..self.config.record_cnt);
        let negative_sampler = Sampler::from(
            &self.config.distribution,
            self.config.record_cnt..self.config.record_cnt * 2,
        );

        let mut guard = crossbeam_epoch::pin();
        let mut scan_buffer = vec![(0, SmallTuple::default()); 32];

        context.wait_for_start();

        let mut op_cnt = 0;
        while context.is_running() {
            if op_cnt % 4096 == 0 {
                guard.repin();
            }
            let op = self.config.workload_mix.gen(&mut small_rng);
            match op {
                Workload::Read => {
                    let key = sampler.sample(&mut small_rng);
                    let murmured_key = murmur64(key);
                    if let Ok(lock) = self.db.lock(&murmured_key) {
                        match self.db.get(&murmured_key, &lock, &guard) {
                            Ok(val) => {
                                assert_eq!(murmured_key, val.primary_key());
                            }
                            Err(e) => {
                                match e {
                                    QueryError::Locked => {}
                                    QueryError::TupleNotFound => {
                                        panic!(
                                            "Value must exists, key: {}, murmured: {}",
                                            key, murmured_key
                                        );
                                    }
                                }
                                continue;
                            }
                        }
                    }
                }
                Workload::NegativeRead => {
                    let key = negative_sampler.sample(&mut small_rng);
                    let murmured_key = murmur64(key);
                    if let Ok(lock) = self.db.lock(&murmured_key) {
                        match self.db.get(&murmured_key, &lock, &guard) {
                            Ok(_val) => {
                                panic!(
                                    "Value must not exists, key: {}, murmured: {}",
                                    key, murmured_key
                                );
                            }
                            Err(e) => match e {
                                QueryError::Locked => {}
                                QueryError::TupleNotFound => {} // expected error
                            },
                        }
                    }
                }
                Workload::Update => {
                    let key = sampler.sample(&mut small_rng);
                    let murmured_key = murmur64(key);
                    let mut tuple = SmallTuple::new(murmured_key);
                    tuple.mutate();

                    if let Ok(lock) = self.db.lock_mut(&murmured_key) {
                        match self.db.update(&murmured_key, tuple, &lock, &guard) {
                            Ok(old) => {
                                assert_eq!(murmured_key, old.unwrap().primary_key());
                            }
                            Err(e) => {
                                match e {
                                    QueryError::Locked => {}
                                    QueryError::TupleNotFound => {
                                        panic!(
                                            "Value must exists, key: {}, murmured: {}",
                                            key, murmured_key
                                        );
                                    }
                                }
                                continue;
                            }
                        }
                    } else {
                        continue;
                    }
                }
                Workload::Scan => {
                    let key = sampler.sample(&mut small_rng);
                    let murmued_key = murmur64(key);
                    let scan_cnt = 30;
                    let low_key = murmued_key;
                    match self.db.scan(&low_key, scan_cnt, &mut scan_buffer, &guard) {
                        Ok(scanned) => {
                            assert!(scanned <= scan_cnt,);
                            for i in 0..scanned {
                                let v = &scan_buffer[i].1;
                                assert!(v.primary_key() >= low_key);
                            }
                        }
                        Err(_e) => continue,
                    };
                }
            }
            op_cnt += 1;
        }

        MicroBenchResult::new(op_cnt, metric::get_tls_recorder().clone())
    }

    fn on_iteration_finished(&mut self, cur_iter: usize) {
        if cur_iter >= 2 {
            let rate = self.config.promotion_rate.unwrap_or_else(|| 5);
            utils::set_promotion_rate(rate);
            two_tree_storage::set_promotion_rate(rate);
        }
    }

    fn load(&mut self) -> Option<serde_json::Value> {
        let evict_batch = self.config.evict_batch.unwrap_or_else(|| 1);
        two_tree_storage::set_evict_cnt_per_batch(evict_batch);

        utils::set_remote_memory_delay(Duration::from_nanos(self.config.delay_nanos as u64));

        // let record_per_thread = self.config.record_cnt / LOAD_THREAD;
        let record_cnt = self.config.record_cnt;
        let mut metrics = metric::TlsRecorder::default();
        std::thread::scope(|s| {
            let mut handlers = Vec::new();
            for t in 0..LOAD_THREAD {
                let db = &self.db;
                let h = s.spawn(move || {
                    metric::get_tls_recorder().reset();
                    let guard = crossbeam_epoch::pin();
                    // for k in (t..record_cnt).step_by(LOAD_THREAD) {
                    // Insert hot keys first.
                    let record_per_thread = record_cnt / LOAD_THREAD;
                    for k in (t * record_per_thread)..((t + 1) * record_per_thread) {
                        'insert_loop: loop {
                            let key = k;
                            let key = murmur64(key);
                            let val = SmallTuple::new(key);

                            if let Ok(lock) = db.lock_mut(&key) {
                                let rv = db.insert(val, &lock, &guard);
                                match rv {
                                    Ok(_) => break 'insert_loop,
                                    Err(e) => match e {
                                        QueryError::Locked => {}
                                        QueryError::TupleNotFound => {
                                            unreachable!()
                                        }
                                    },
                                }
                            } else {
                                continue 'insert_loop;
                            }
                        }
                    }
                    metric::get_tls_recorder().clone()
                });
                handlers.push(h);
            }
            for h in handlers {
                metrics += h.join().unwrap();
            }
        });

        unsafe {
            self.db.bench_finished_loading_hint();
        }

        // we set to real promotion rate after the iteration 3
        utils::set_promotion_rate(15);
        two_tree_storage::set_promotion_rate(15);

        let repeat = 10;
        let start = std::time::Instant::now();
        for _i in 0..repeat {
            btree_olc::utils::remote_delay();
        }
        let elapsed = start.elapsed() / repeat;

        let metric_enabled = cfg!(feature = "metrics");
        Some(serde_json::json!({ "metrics_enabled": metric_enabled,
                                "delay": elapsed.as_nanos() as u64,
                                "metrics": metrics,
                                "index_type": INDEX_TYPE,}))
    }

    fn cleanup(&mut self) -> Option<serde_json::Value> {
        // self.db.stats()
        None
    }
}

// type TestIndex<K, V, A> = congee::Art<K, V, A>;
// const INDEX_TYPE: &str = "Art";

// type TestIndex<K, V, M> = btree_olc::btree::Btree<K, V, M>;
const INDEX_TYPE: &str = "Btree";

fn main() {
    let config = DBConfig::load().expect("Failed to parse config!");
    for c in config {
        let repeat = c.repeat.unwrap_or(7);
        let results = match c.sut {
            SystemType::OneTree => {
                let mut pool =
                    ThreeTierOneTreePool::new(c.dram_size_mb * MB, c.remote_size_mb * MB);
                let tree = ThreeTierOneTree::new(&mut pool);
                let mut sut = TestDB::new(c.clone(), tree);
                let rv = shumai::run(&mut sut, &c, repeat);
                std::mem::forget(sut);
                rv
            }
            SystemType::TwoTreeLower => {
                let mut pool =
                    TwoTreeLowerBufferPool::new(c.dram_size_mb * MB, c.remote_size_mb * MB);
                let tree = TwoTreeLowerBtree::<_>::new(&mut pool);
                let mut sut = TestDB::new(c.clone(), tree);
                let rv = shumai::run(&mut sut, &c, repeat);
                std::mem::forget(sut);
                rv
            }
            SystemType::TwoTreeUpper => {
                let mut pool =
                    TwoTreeUpperBufferPool::new(c.dram_size_mb * MB, c.remote_size_mb * MB);
                let tree = TwoTreeUpperBtree::<_>::new(&mut pool);
                let mut sut = TestDB::new(c.clone(), tree);
                let rv = shumai::run(&mut sut, &c, repeat);
                std::mem::forget(sut);
                rv
            }
            SystemType::TwoTreeUpperBlind => {
                let mut pool =
                    TwoTreeUpperBlindBufferPool::new(c.dram_size_mb * MB, c.remote_size_mb * MB);
                let tree = TwoTreeUpperBlindBtree::<_>::new(&mut pool);
                let mut sut = TestDB::new(c.clone(), tree);
                let rv = shumai::run(&mut sut, &c, repeat);
                std::mem::forget(sut);
                rv
            }
            SystemType::ThreeTree => {
                let mut pool = ThreeTreePool::new(c.dram_size_mb * MB, c.remote_size_mb * MB);
                let tree = ThreeTreeBtree::<_>::new(&mut pool);
                let mut sut = TestDB::new(c.clone(), tree);
                let rv = shumai::run(&mut sut, &c, repeat);
                std::mem::forget(sut);
                rv
            }
            SystemType::TwoTierOneTree => {
                let mut pool = TwoTierOneTreePool::new(c.dram_size_mb * MB, c.remote_size_mb * MB);
                let tree = TwoTierOneTree::<_>::new(&mut pool);
                let mut sut = TestDB::new(c.clone(), tree);
                let rv = shumai::run(&mut sut, &c, repeat);
                std::mem::forget(sut);
                rv
            }
        };
        results.write_json().expect("Failed to write results!");
    }
}
