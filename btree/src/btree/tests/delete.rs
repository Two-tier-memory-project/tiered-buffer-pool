use crate::btree::Btree;

use super::{get_memory_source, LargeKey};

#[test]
fn two_node_delete() {
    let mem = get_memory_source();
    let tree = Btree::new(mem);

    let guard = tree.pin();
    let insert_cnt = 28;
    for i in 0..insert_cnt {
        let key = LargeKey::new(i);
        tree.insert(key, i, &guard).unwrap();
    }

    for i in 0..insert_cnt {
        let key = LargeKey::new(i);
        let val = tree.get(&key, &guard);
        assert_eq!(val, Some(i));
    }

    for i in 0..insert_cnt / 2 {
        let key = LargeKey::new(i);
        let v = tree.remove(&key, &guard);
        assert_eq!(v, Some(i));
    }

    for i in 0..insert_cnt {
        let key = LargeKey::new(i);
        if i < insert_cnt / 2 {
            let v = tree.get(&key, &guard);
            assert!(v.is_none());
        } else {
            let v = tree.get(&key, &guard);
            assert_eq!(v, Some(i));
        }
    }
}
