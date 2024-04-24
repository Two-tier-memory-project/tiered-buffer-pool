#![no_main]
#![feature(generic_const_exprs)]
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use btree_olc::btree::Btree;
use std::collections::BTreeMap;

#[derive(Arbitrary, Debug)]
enum MapMethod {
    Get { key: usize },
    Insert { key: usize, val: usize },
    Update { key: usize, val: usize },
    Range { low_v: usize, cnt: u16 },
    Delete { key: usize },
}

#[derive(PartialEq, Debug, Eq, PartialOrd, Ord, Clone)]
struct LargeKey {
    key: usize,
    _padding: [u8; 256],
}

impl LargeKey {
    fn new(key: usize) -> Self {
        Self {
            key,
            _padding: [0; 256],
        }
    }
}

fuzz_target!(|ops: Vec<MapMethod>| {
    let mut std_btree = BTreeMap::<LargeKey, usize>::new();
    let btree_olc = Btree::default();

    for m_c in ops.chunks(1024) {
        let pin = btree_olc.pin();
        for m in m_c {
            match m {
                MapMethod::Get { key } => {
                    let large_key = LargeKey::new(*key);
                    let btree_val = std_btree.get(&large_key);
                    let bt_olc_val = btree_olc.get(&large_key, &pin);
                    match btree_val {
                        Some(v) => assert_eq!(*v, bt_olc_val.unwrap()),
                        None => assert!(bt_olc_val.is_none()),
                    }
                }
                MapMethod::Insert { key, val } => {
                    let large_key = LargeKey::new(*key);
                    std_btree.insert(large_key.clone(), *val);
                    btree_olc.insert(large_key, *val, &pin).unwrap();
                }
                MapMethod::Update { key, val } => {
                    let large_key = LargeKey::new(*key);
                    let old_bt = std_btree.get_mut(&large_key);
                    let old_art = btree_olc.compute_if_present(&large_key, |_v| *val, &pin);
                    if let Some(old_bt) = old_bt {
                        assert_eq!(old_art, Some((*old_bt, *val)));
                        *old_bt = *val;
                    } else {
                        assert_eq!(old_art, None);
                    }
                }
                MapMethod::Delete { key } => {
                    let large_key = LargeKey::new(*key);
                    std_btree.remove(&large_key);
                    btree_olc.remove(&large_key, &pin);
                }
                MapMethod::Range { low_v, cnt } => {
                    let cnt = *cnt as usize;

                    // prevent integer overflow
                    let high_v = if (usize::MAX - low_v) <= cnt {
                        usize::MAX
                    } else {
                        low_v + cnt
                    };

                    let low = LargeKey::new(*low_v);
                    let high = LargeKey::new(high_v);

                    let bt_range = btree_olc.range(&low, &high, &pin);
                    let bt_range = bt_range.collect::<Vec<(LargeKey, usize)>>();
                    let std_range: Vec<(&LargeKey, &usize)> = std_btree.range(low..high).collect();

                    assert_eq!(bt_range.len(), std_range.len());

                    for (st, bt) in std_range.iter().zip(bt_range.iter()) {
                        assert_eq!(*st.0, bt.0);
                        assert_eq!(*st.1, bt.1);
                    }
                }
            }
        }
    }

    let guard = btree_olc.pin();
    for (k, v) in std_btree.iter() {
        assert_eq!(btree_olc.get(k, &guard).unwrap(), *v);
    }
});
