#[derive(Clone, Debug, PartialEq)]
pub struct ReferencedTuple<V> {
    pub(crate) value: V,
    pub(crate) referenced: bool,
}

impl<V> ReferencedTuple<V> {
    pub(crate) fn new_referenced(value: V) -> Self {
        Self {
            value,
            referenced: true,
        }
    }

    pub(crate) fn reference(&mut self) {
        self.referenced = true;
    }

    pub(crate) fn clear_reference(&mut self) {
        self.referenced = false;
    }
}

