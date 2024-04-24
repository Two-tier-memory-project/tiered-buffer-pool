use crate::{btree::Btree, buffer_pool::InMemory, PAGE_SIZE};
use rand::{
    prelude::{SliceRandom, StdRng},
    Rng, SeedableRng,
};

#[cfg(feature = "shuttle")]
use shuttle::{sync::Arc, thread};

#[cfg(not(feature = "shuttle"))]
use std::{sync::Arc, thread};

use super::{get_memory_source, LargeKey};

#[test]
fn basic_ops() {
    let mem = get_memory_source();
    let tree = Btree::new(mem);

    let guard = tree.pin();
    let insert_cnt = 30_000;
    for i in 0..insert_cnt {
        let key = LargeKey::new(i);
        tree.insert(key, i, &guard).unwrap();
    }

    for i in 0..insert_cnt {
        if i == 1 {
            println!("");
        }
        let key = LargeKey::new(i);
        let val = tree.get(&key, &guard);
        assert_eq!(val, Some(i));
    }
}

#[test]
fn small_playground() {
    let mem = get_memory_source();
    let tree = Btree::new(mem);
    let guard = tree.pin();
    tree.insert(LargeKey::new(0), 0, &guard).unwrap();
    let low = LargeKey::new(0);
    let high = LargeKey::new(0);
    let iter = tree.range(&low, &high, &guard);
    assert_eq!(iter.count(), 0);
}

#[test]
fn concurrent_insert() {
    let mem = Arc::new(get_memory_source());
    let key_cnt_per_thread = 500;
    let n_thread = 4;
    let mut key_space = Vec::with_capacity(key_cnt_per_thread * n_thread);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }
    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let key_space = Arc::new(key_space);

    let tree = Arc::new(Btree::<LargeKey, usize, _>::new(mem));

    let mut handlers = Vec::new();
    for t in 0..n_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();

        handlers.push(thread::spawn(move || {
            let guard = crossbeam_epoch::pin();
            for i in 0..key_cnt_per_thread {
                let idx = t * key_cnt_per_thread + i;
                let val = key_space[idx];
                tree.insert(LargeKey::new(val), val, &guard).unwrap();
            }
        }));
    }

    for h in handlers.into_iter() {
        h.join().unwrap();
    }

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        let val = tree.get(&LargeKey::new(*v), &guard).unwrap();
        assert_eq!(val, *v);
    }
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_concurrent_insert() {
    let mut config = shuttle::Config::default();
    config.max_steps = shuttle::MaxSteps::None;
    let mut runner = shuttle::PortfolioRunner::new(true, config);
    runner.add(shuttle::scheduler::PctScheduler::new(5, 8_000));
    runner.add(shuttle::scheduler::PctScheduler::new(5, 8_000));
    runner.add(shuttle::scheduler::RandomScheduler::new(8_000));
    runner.add(shuttle::scheduler::RandomScheduler::new(8_000));

    runner.run(concurrent_insert);
}

#[test]
fn test_concurrent_insert_read() {
    let mem = get_memory_source();
    let key_cnt_per_thread = 500;
    let w_thread = 2;
    let r_thread = 1;
    let mut key_space = Vec::with_capacity(key_cnt_per_thread * w_thread);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let key_space = Arc::new(key_space);

    let tree = Arc::new(Btree::new(mem));

    let mut handlers = Vec::new();
    for t in 0..w_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let guard = crossbeam_epoch::pin();
            for i in 0..key_cnt_per_thread {
                let idx = t * key_cnt_per_thread + i;
                let val = key_space[idx];
                tree.insert(LargeKey::new(val), val, &guard).unwrap();
            }
        }));
    }

    for t in 0..r_thread {
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let mut r = StdRng::seed_from_u64(10 + t);
            let guard = crossbeam_epoch::pin();
            for _i in 0..key_cnt_per_thread {
                let val = r.gen_range(0..(key_cnt_per_thread * w_thread));
                if let Some(v) = tree.get(&LargeKey::new(val), &guard) {
                    assert_eq!(v, val);
                }
            }
        }));
    }

    for h in handlers.into_iter() {
        h.join().unwrap();
    }

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        let val = tree.get(&LargeKey::new(*v), &guard).unwrap();
        assert_eq!(val, *v);
    }
}

#[test]
#[cfg(feature = "shuttle")]
fn shuttle_concurrent_read() {
    let config = shuttle::Config::default();
    let mut runner = shuttle::PortfolioRunner::new(true, config);
    runner.add(shuttle::scheduler::PctScheduler::new(5, 8_000));
    runner.add(shuttle::scheduler::PctScheduler::new(5, 8_000));
    runner.add(shuttle::scheduler::RandomScheduler::new(8_000));
    runner.add(shuttle::scheduler::RandomScheduler::new(8_000));

    runner.run(test_concurrent_insert_read);
}

#[test]
fn concurrent_all() {
    let mem = get_memory_source();
    let key_cnt_per_thread = 300;
    let delete_cnt_per_thread = key_cnt_per_thread / 2;
    let insert_thread = 2;
    let read_thread = 1;
    let update_thread = 1;
    let delete_thread = 2;

    let mut insert_key_space = Vec::with_capacity(key_cnt_per_thread * insert_thread);
    for i in 0..insert_key_space.capacity() {
        insert_key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    insert_key_space.shuffle(&mut r);

    let key_space = Arc::new(insert_key_space);

    let tree = Arc::new(Btree::new(mem));

    let mut handlers = Vec::new();
    for t in 0..insert_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let guard = crossbeam_epoch::pin();
            for i in 0..key_cnt_per_thread {
                let idx = t * key_cnt_per_thread + i;
                let val = key_space[idx];
                tree.insert(LargeKey::new(val), val, &guard).unwrap();
            }
        }));
    }

    let mut delete_keys = Vec::with_capacity(delete_cnt_per_thread * delete_thread);
    for i in 0..delete_keys.capacity() {
        delete_keys.push(i);
    }

    delete_keys.shuffle(&mut r);

    let key_space = Arc::new(delete_keys);
    for t in 0..delete_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let guard = crossbeam_epoch::pin();
            for i in 0..delete_cnt_per_thread {
                let idx = t * delete_cnt_per_thread + i;
                let val = key_space[idx];
                tree.remove(&LargeKey::new(val), &guard);
            }
        }));
    }

    for t in 0..read_thread {
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let mut r = StdRng::seed_from_u64(10 + t);
            let guard = crossbeam_epoch::pin();
            for _i in 0..key_cnt_per_thread {
                let val = r.gen_range(0..(key_cnt_per_thread * insert_thread));
                if let Some(v) = tree.get(&LargeKey::new(val), &guard) {
                    assert!(v == val || v == (val / 2 * 2));
                }
            }
        }));
    }

    for t in 0..update_thread {
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let mut r = StdRng::seed_from_u64(10 + t);
            let guard = crossbeam_epoch::pin();
            for _i in 0..key_cnt_per_thread {
                let val = r.gen_range(0..(key_cnt_per_thread * insert_thread));
                tree.compute_if_present(&LargeKey::new(val), |v| (*v / 2) * 2, &guard);
            }
        }));
    }

    for h in handlers.into_iter() {
        h.join().unwrap();
    }
}

#[test]
#[cfg(feature = "shuttle")]
fn shuttle_concurrent_all() {
    let config = shuttle::Config::default();
    let mut runner = shuttle::PortfolioRunner::new(true, config);
    runner.add(shuttle::scheduler::PctScheduler::new(5, 8_000));
    runner.add(shuttle::scheduler::PctScheduler::new(5, 8_000));
    runner.add(shuttle::scheduler::RandomScheduler::new(8_000));
    runner.add(shuttle::scheduler::RandomScheduler::new(8_000));

    runner.run(concurrent_all);
}

#[test]
fn insert_full_delete() {
    let mem = Arc::new(InMemory::new(
        douhua::MemType::DRAM,
        (PAGE_SIZE * 64) as isize,
    ));
    let key_cnt_per_thread = 300;
    let w_thread = 4;

    let tree = Arc::new(Btree::new(mem));

    let mut handlers = Vec::new();
    for t in 0..w_thread {
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let mut r = StdRng::seed_from_u64(t);
            let guard = crossbeam_epoch::pin();
            let mut inserted_set = Vec::new();
            for _i in 0..key_cnt_per_thread {
                if r.gen_bool(0.2) && inserted_set.len() > 0 {
                    // Delete
                    let idx = r.gen_range(0..inserted_set.len());
                    let val = inserted_set.swap_remove(idx);
                    let old = tree
                        .remove(&LargeKey::new(val), &guard)
                        .expect("Must exists");
                    assert_eq!(old, val);
                } else {
                    // Insert
                    let val = r.gen();
                    if let Ok(_) = tree.insert(LargeKey::new(val), val, &guard) {
                        inserted_set.push(val);
                    };
                }
            }
            inserted_set
        }));
    }

    let mut inserted_set = Vec::new();
    for h in handlers.into_iter() {
        let mut thread_set = h.join().unwrap();
        inserted_set.append(&mut thread_set);
    }

    println!("inserted_set: {:?}", inserted_set.len());

    let guard = crossbeam_epoch::pin();
    for v in inserted_set.iter() {
        let val = tree.get(&LargeKey::new(*v), &guard).unwrap();
        assert_eq!(val, *v);
    }
}

#[test]
#[cfg(feature = "shuttle")]
fn shuttle_insert_full_delete() {
    let config = shuttle::Config::default();
    let mut runner = shuttle::PortfolioRunner::new(true, config);
    runner.add(shuttle::scheduler::PctScheduler::new(5, 4_000));
    runner.add(shuttle::scheduler::PctScheduler::new(5, 4_000));
    runner.add(shuttle::scheduler::RandomScheduler::new(4_000));
    runner.add(shuttle::scheduler::RandomScheduler::new(4_000));

    runner.run(insert_full_delete);
}

#[test]
#[cfg(feature = "shuttle")]
fn shuttle_replay() {
    // shuttle::replay(shuttle_insert_full_delete, "91039004f9b4a0d0a1aeababc201000000000022422020022822846226244424662868422224824662424828288448224248226824264686686462426882222466264242482864288268484428888868284666842462842886224888446624488628662426264626226826866284426248686882268682664642642482222426284648846862448882626642284644862822242882846246624688646262484664848666626466644684244228242288622228684286882668848682468664484468242866288488446286268864228246888286466466828422626268828464846622288822262248286466848228266442884246422842286488686886844248644288222642842244846484648888222444844464");
}
