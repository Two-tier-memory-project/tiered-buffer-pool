use crate::btree::Btree;

use super::{get_memory_source, LargeKey};

#[test]
fn smoke() {}

#[test]
fn small_scan() {
    let mem = get_memory_source();
    let tree = Btree::new(mem);

    let guard = tree.pin();

    let insert_cnt = 5000;

    for i in 0..insert_cnt {
        let key = LargeKey::new(i);
        tree.insert(key, i, &guard).unwrap();
    }

    let low = LargeKey::new(0);
    let high = LargeKey::new(insert_cnt);
    let iter = tree.range(&low, &high, &guard);

    for (i, (key, val)) in iter.enumerate() {
        assert_eq!(i, val);
        assert_eq!(key, LargeKey::new(i));
    }

    let high = LargeKey::new(insert_cnt / 2);
    let iter = tree.range(&low, &high, &guard);
    for (i, (key, val)) in iter.enumerate() {
        assert_eq!(i, val);
        assert_eq!(key, LargeKey::new(i));
        assert!(i <= insert_cnt / 2);
    }

    let low = LargeKey::new(insert_cnt / 4);
    let iter = tree.range(&low, &high, &guard);
    for (i, (key, val)) in iter.enumerate() {
        assert_eq!(i + low.key, val);
        assert_eq!(key, LargeKey::new(val));
        assert!(val <= insert_cnt / 2);
        assert!(val >= insert_cnt / 4);
    }
}
