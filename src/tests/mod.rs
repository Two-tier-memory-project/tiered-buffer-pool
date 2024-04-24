use std::{sync::Arc, thread};

use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};

use crate::{
    one_tree::ThreeTierOneTree, three_tree_btree::ThreeTreeBtree,
    two_tree_btree::TwoTreeLowerBtree, two_tree_btree::TwoTreeUpperBtree, BufferPoolDB, DBTuple,
    QueryError, TableInterface, ThreeTierOneTreePool, ThreeTreePool, TwoTreeLowerBufferPool,
    TwoTreeUpperBufferPool,
};

#[test]
fn smoke() {}

#[derive(PartialEq, Eq, Debug, Clone)]
struct LargeTuple {
    val: usize,
    _padding: [usize; 64],
}

impl DBTuple for LargeTuple {
    fn primary_key(&self) -> usize {
        self.val
    }
}

impl LargeTuple {
    fn new(val: usize) -> Self {
        Self {
            val,
            _padding: [0; 64],
        }
    }
}

fn small_test(tree: &impl TableInterface<LargeTuple>, insert_cnt: usize) {
    let guard = crossbeam_epoch::pin();

    for i in 0..insert_cnt {
        let key = LargeTuple::new(i);
        let lock = tree.lock_mut(&key.primary_key()).unwrap();
        tree.insert(key, &lock, &guard).unwrap();
    }

    for i in 0..insert_cnt {
        let key = LargeTuple::new(i);
        let wl = tree.lock(&key.primary_key()).expect("Lock should succeed.");
        let val = tree
            .get(&key.primary_key(), &wl, &guard)
            .expect("Value should exist.");

        assert_eq!(val, key);
    }

    for i in insert_cnt..(insert_cnt + 10) {
        let key = LargeTuple::new(i);
        let wl = tree.lock(&key.primary_key()).expect("Lock should succeed.");
        let val = tree.get(&key.primary_key(), &wl, &guard);

        assert!(matches!(val, Err(QueryError::TupleNotFound)));
    }
}

#[test]
fn one_tree_small() {
    let insert_cnt = 50_000;
    let mut pool = ThreeTierOneTreePool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = ThreeTierOneTree::new(&mut pool);
    small_test(&tree, insert_cnt);
}

#[test]
fn two_tree_lower_small() {
    let insert_cnt = 50_000;
    let mut pool = TwoTreeLowerBufferPool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = TwoTreeLowerBtree::<_>::new(&mut pool);
    small_test(&tree, insert_cnt);
}

#[test]
fn two_tree_upper_small() {
    let insert_cnt = 50_000;
    let mut pool = TwoTreeUpperBufferPool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = TwoTreeUpperBtree::<_>::new(&mut pool);
    small_test(&tree, insert_cnt);
}

#[test]
fn three_tree_small() {
    let insert_cnt = 50_000;
    let mut pool = ThreeTreePool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = ThreeTreeBtree::<_>::new(&mut pool);
    small_test(&tree, insert_cnt);
}

fn concurrent_insert<T: TableInterface<LargeTuple> + Send + Sync + 'static>(
    tree: Arc<T>,
    insert_cnt: usize,
) {
    let n_thread = 4;
    let key_cnt_per_thread = insert_cnt / n_thread;
    let mut key_space = Vec::with_capacity(key_cnt_per_thread * n_thread);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }
    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let key_space = Arc::new(key_space);

    let mut handlers = Vec::new();
    for t in 0..n_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();

        handlers.push(thread::spawn(move || {
            let guard = crossbeam_epoch::pin();
            for i in 0..key_cnt_per_thread {
                let idx = t * key_cnt_per_thread + i;
                let val = LargeTuple::new(key_space[idx]);
                let lock = tree
                    .lock_mut(&val.primary_key())
                    .expect("Lock should succeed.");
                tree.insert(val, &lock, &guard)
                    .expect("Inserting a non-existing key can't fail.");
            }
        }));
    }

    for h in handlers.into_iter() {
        h.join().unwrap();
    }

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        let wl = tree.lock(v).expect("Locking should succeed.");
        let expected = LargeTuple::new(*v);
        let actual = tree.get(&expected.primary_key(), &wl, &guard).unwrap();
        assert_eq!(expected, actual);
    }
}

#[test]
fn one_tree_concurrent_insert() {
    let insert_cnt = 20_000;
    let mut pool = ThreeTierOneTreePool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = Arc::new(ThreeTierOneTree::new(&mut pool));
    concurrent_insert(tree, insert_cnt);
}

#[test]
fn two_tree_lower_concurrent_insert() {
    let insert_cnt = 20_000;
    let mut pool = TwoTreeLowerBufferPool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = Arc::new(TwoTreeLowerBtree::<_>::new(&mut pool));
    concurrent_insert(tree, insert_cnt);
}

#[test]
fn two_tree_upper_concurrent_insert() {
    let insert_cnt = 20_000;
    let mut pool = TwoTreeUpperBufferPool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = Arc::new(TwoTreeUpperBtree::<_>::new(&mut pool));
    concurrent_insert(tree, insert_cnt);
}

#[test]
fn three_tree_concurrent_insert() {
    let insert_cnt = 20_000;
    let mut pool = ThreeTreePool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = Arc::new(ThreeTreeBtree::<_>::new(&mut pool));
    concurrent_insert(tree, insert_cnt);
}

use rand::Rng;
fn concurrent_get<T: TableInterface<LargeTuple> + Send + Sync + 'static>(
    tree: Arc<T>,
    insert_cnt: usize,
) {
    let n_thread = 4;
    let op_count = 5_000 * n_thread;

    let mut key_space = Vec::with_capacity(insert_cnt);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }
    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    for i in key_space.iter() {
        let val = LargeTuple::new(*i);
        let lock = tree
            .lock_mut(&val.primary_key())
            .expect("Lock should succeed.");
        tree.insert(val, &lock, &crossbeam_epoch::pin()).unwrap();
    }

    let key_space = Arc::new(key_space);

    let mut handlers = Vec::new();
    for t in 0..n_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();

        handlers.push(thread::spawn(move || {
            let guard = crossbeam_epoch::pin();
            let mut rng = StdRng::seed_from_u64(t as u64);
            for _i in 0..op_count {
                let idx = rng.gen_range(0..key_space.len());
                let expected = LargeTuple::new(key_space[idx]);
                if let Ok(l) = tree.lock(&expected.primary_key()) {
                    match tree.get(&expected.primary_key(), &l, &guard) {
                        Ok(v) => assert_eq!(expected, v),
                        _ => {
                            panic!("unexpected error")
                        }
                    };
                }
            }
        }));
    }

    for h in handlers.into_iter() {
        h.join().unwrap();
    }
}

#[test]
fn one_tree_concurrent_get() {
    let insert_cnt = 20_000;
    let mut pool = ThreeTierOneTreePool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = Arc::new(ThreeTierOneTree::new(&mut pool));
    concurrent_get(tree, insert_cnt);
}

#[test]
fn two_tree_lower_concurrent_get() {
    let insert_cnt = 20_000;
    let mut pool = TwoTreeLowerBufferPool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = Arc::new(TwoTreeLowerBtree::<_>::new(&mut pool));
    concurrent_get(tree, insert_cnt);
}

#[test]
fn two_tree_upper_concurrent_get() {
    let insert_cnt = 20_000;
    let mut pool = TwoTreeUpperBufferPool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = Arc::new(TwoTreeUpperBtree::<_>::new(&mut pool));
    concurrent_get(tree, insert_cnt);
}

#[test]
fn three_tree_concurrent_get() {
    let insert_cnt = 20_000;
    let mut pool = ThreeTreePool::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    let tree = Arc::new(ThreeTreeBtree::<_>::new(&mut pool));
    concurrent_get(tree, insert_cnt);
}
