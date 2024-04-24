use crate::DBTuple;
use rand_distr::{Distribution, Uniform};
use selfsimilar::SelfSimilarDistribution;
use std::ops::Range;

pub mod config;
pub mod results;
pub mod tatp;

pub enum Sampler {
    Uniform(Uniform<usize>),
    SelfSimilar(SelfSimilarDistribution),
    HotSpot(Uniform<usize>),
    Zipf((zipf::ZipfDistribution, usize)),
}

impl Sampler {
    pub fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> usize {
        match self {
            Self::SelfSimilar(s) => s.sample(rng),
            Self::Uniform(u) => u.sample(rng),
            Self::HotSpot(u) => u.sample(rng),
            Self::Zipf(z) => z.0.sample(rng) - 1 + z.1, // Zipf distribution starts with 1
        }
    }

    pub fn from(dist: &config::Distribution, item_range: Range<usize>) -> Self {
        match dist {
            config::Distribution::SelfSimilar(skew) => Sampler::SelfSimilar(
                SelfSimilarDistribution::new(item_range.start, item_range.end, *skew),
            ),
            config::Distribution::Uniform => Sampler::Uniform(Uniform::from(item_range)),
            config::Distribution::HotSpot(skew) => {
                assert!(*skew <= 1.0);

                let end = (item_range.start as f64
                    + (item_range.end - item_range.start) as f64 * skew)
                    as usize;
                let hotspot_range = item_range.start..end;
                Sampler::HotSpot(Uniform::from(hotspot_range))
            }
            config::Distribution::Zipf(s) => {
                let num_element = item_range.end - item_range.start;
                Sampler::Zipf((
                    zipf::ZipfDistribution::new(num_element, *s).unwrap(),
                    item_range.start,
                ))
            }
        }
    }
}

#[repr(align(1))]
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct SmallTuple {
    pub val: [usize; 16], // 128 bytes
}

impl SmallTuple {
    pub fn new(val: usize) -> Self {
        Self { val: [val; 16] }
    }

    /// Modify the tuple so that it is different from the original
    pub fn mutate(&mut self) {
        self.val[1] = self.val[0] + 1;
    }
}

impl DBTuple for SmallTuple {
    fn primary_key(&self) -> usize {
        self.val[0]
    }
}

pub fn murmur64(mut h: usize) -> usize {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}

#[test]
fn test_unique_murmur() {
    use std::collections::HashMap;
    let total_items = 50_000_000;
    let mut ht = HashMap::new();
    for i in 0..total_items {
        let key = murmur64(i);
        assert!(ht.insert(key, i).is_none());
    }
}

#[test]
fn test_zipf() {
    use rand::prelude::*;
    let mut rng = rand::thread_rng();
    let zipf = zipf::ZipfDistribution::new(1000, 0.9).unwrap();
    let mut ht = std::collections::HashMap::new();
    for _ in 0..100_000 {
        let key = zipf.sample(&mut rng);
        *ht.entry(key).or_insert(0) += 1;
    }
    let mut rv = ht.iter().collect::<Vec<_>>();
    rv.sort_by(|a, b| a.0.cmp(b.0));
    println!("{:?}", rv);
    // for i in ht.iter() {
    //     println!("{:?}", i);
    // }
    // println!("{:?}", ht);
}
