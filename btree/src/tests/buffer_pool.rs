use rand::{rngs::StdRng, SeedableRng};
#[cfg(feature = "shuttle")]
use shuttle::{sync::Arc, thread};
#[cfg(not(feature = "shuttle"))]
use std::{sync::Arc, thread};

use crate::{
    btree::basic::BaseNode,
    buffer_pool::{
        BufferPool, InMemory, ThreeTierBufferPool, TwoTierBufferPool, TwoTierMemory,
        TwoTierMemoryBlind,
    },
    utils::Backoff,
};

const SHARD_CNT: usize = 1;
const BUFFER_SIZE: usize = jasmine_db::SEGMENT_SIZE * SHARD_CNT;

fn get_three_tier_buffer_pool() -> ThreeTierBufferPool<SHARD_CNT> {
    ThreeTierBufferPool::<SHARD_CNT>::new(BUFFER_SIZE, BUFFER_SIZE, true)
}

fn get_two_tier_buffer_pool() -> TwoTierBufferPool<SHARD_CNT> {
    TwoTierBufferPool::<SHARD_CNT>::new(BUFFER_SIZE * 2, douhua::MemType::DRAM, false)
}

fn get_two_tier_memory_pool() -> TwoTierMemory<SHARD_CNT> {
    TwoTierMemory::<SHARD_CNT>::new(BUFFER_SIZE as isize, BUFFER_SIZE as isize)
}

fn get_two_tier_memory_pool_blind() -> TwoTierMemoryBlind {
    TwoTierMemoryBlind::new(BUFFER_SIZE as isize, BUFFER_SIZE as isize)
}

fn get_in_memory_pool() -> InMemory {
    InMemory::new(douhua::MemType::DRAM, BUFFER_SIZE as isize * 2)
}

fn test_all_buffer_pools<T>(test_func: T)
where
    T: Fn(Box<dyn BufferPool>),
{
    test_func(Box::new(get_three_tier_buffer_pool()));
    test_func(Box::new(get_two_tier_buffer_pool()));
    test_func(Box::new(get_two_tier_memory_pool()));
    test_func(Box::new(get_two_tier_memory_pool_blind()));
    test_func(Box::new(get_in_memory_pool()));
}

#[should_panic]
#[test]
fn basic_buffer_pool() {
    let pool = ThreeTierBufferPool::<SHARD_CNT>::new(1024 * 4, 1024 * 4, true);

    let _ = pool.reserve_page(crate::btree::PageType::Inner);
}

#[test]
fn reserve_and_resolve() {
    let test_core = |pool: Box<dyn BufferPool>| {
        let node = pool.reserve_page(crate::btree::PageType::Inner).unwrap();
        let pid = node.as_ref().pid();

        std::mem::drop(node);

        let v = pool.resolve_pid(pid.clone());
        assert_eq!(pid, unsafe { &*v }.pid());
    };
    test_all_buffer_pools(test_core);
}

#[test]
fn reserve_and_resolve_many() {
    let test = |pool: Box<dyn BufferPool>| {
        let mut nodes = Vec::new();
        for _ in 0..100 {
            let node = pool.reserve_page(crate::btree::PageType::Inner).unwrap();
            let pid = node.as_ref().pid();
            let ptr = pool.resolve_pid(pid.clone());
            assert_eq!(ptr as *const BaseNode, node.as_ref());
            assert!(BaseNode::write_lock_new(ptr).is_err());
            nodes.push(node);
        }
    };
    test_all_buffer_pools(test);
}

fn concurrent_buffer(cnt: Arc<std::sync::atomic::AtomicUsize>, pool: Box<dyn BufferPool>) {
    let cnt = cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if cnt % 20 == 0 {
        println!("Shuttle iteration: {}", cnt);
    }
    let pool = Arc::new(pool);
    let reserve_per_thread = 511;
    let read_per_thread = 512;

    let reserved_set = Arc::new(congee::Art::default());
    let mut handles = Vec::new();

    let read_thread = 2;
    let reserve_thread = 2;

    for _ in 0..reserve_thread {
        let pool = pool.clone();
        let reserved = reserved_set.clone();
        let handle = thread::spawn(move || {
            let guard = crossbeam_epoch::pin();
            for _i in 0..reserve_per_thread {
                let node = pool.reserve_page(crate::btree::PageType::Inner).unwrap();
                let pid = node.as_ref().pid();
                let ptr = pool.resolve_pid(pid.clone());
                assert_eq!(ptr as *const BaseNode, node.as_ref());
                assert!(BaseNode::write_lock_new(ptr).is_err());
                reserved.insert(pid.clone(), pid.clone(), &guard).unwrap();
            }
        });
        handles.push(handle);
    }

    for _ in 0..read_thread {
        let pool = pool.clone();
        let reserved = reserved_set.clone();
        let handle = thread::spawn(move || {
            let guard = crossbeam_epoch::pin();

            for t in 0..read_per_thread {
                let mut rng = StdRng::seed_from_u64(t as u64);
                let (_k, old, _new) =
                    if let Some(v) = reserved.compute_on_random(&mut rng, |_k, v| v, &guard) {
                        v
                    } else {
                        continue;
                    };

                let backoff = Backoff::new();
                loop {
                    let ptr = pool.resolve_pid(old.clone());

                    if unsafe { &*ptr }.pid() == old {
                        break;
                    }
                    backoff.spin();
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn plain_concurrent_ops() {
    test_all_buffer_pools(|pool| {
        concurrent_buffer(Arc::new(std::sync::atomic::AtomicUsize::new(0)), pool);
    })
}

#[test]
#[cfg(feature = "shuttle")]
fn shuttle_buffer_ops() {
    let cnt = std::sync::atomic::AtomicUsize::new(0);
    let cnt = Arc::new(cnt);
    let config = shuttle::Config::default();
    let mut runner = shuttle::PortfolioRunner::new(true, config);
    runner.add(shuttle::scheduler::PctScheduler::new(5, 2_000));
    runner.add(shuttle::scheduler::PctScheduler::new(5, 2_000));
    runner.add(shuttle::scheduler::RandomScheduler::new(2_000));
    runner.add(shuttle::scheduler::RandomScheduler::new(2_000));

    runner.run(move || {
        let cnt = cnt.clone();
        // concurrent_buffer(cnt, Box::new(get_three_tier_buffer_pool()));
        // concurrent_buffer(cnt, Box::new(get_two_tier_memory_pool()));
        // concurrent_buffer(cnt, Box::new(get_two_tier_buffer_pool()));
        // concurrent_buffer(cnt, Box::new(get_two_tier_memory_pool_blind()));
        concurrent_buffer(cnt, Box::new(get_in_memory_pool()));
    });
}
