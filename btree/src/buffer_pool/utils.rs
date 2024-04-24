use fxhash::FxBuildHasher;
use std::ops::Deref;

pub(super) struct HashMap<K, V>(dashmap::DashMap<K, V, FxBuildHasher>);

impl<K: std::cmp::Eq + std::hash::Hash, V: std::cmp::Eq + Clone> HashMap<K, V> {
    pub(super) fn new() -> Self {
        Self(dashmap::DashMap::default())
    }

    /// Returns current value.
    pub(super) fn compare_exchange(&self, key: &K, old: &V, new: V) -> Result<V, Option<V>> {
        let v = self.0.get_mut(key);
        if let Some(mut v) = v {
            if *v.value() == *old {
                *v.value_mut() = new.clone();
                Ok(new)
            } else {
                Err(Some(v.value().clone()))
            }
        } else {
            Err(None)
        }
    }
}

impl<K, V> Deref for HashMap<K, V> {
    type Target = dashmap::DashMap<K, V, FxBuildHasher>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
