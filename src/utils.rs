use std::ops::Index;

pub struct EntryVector<T> {
    entry_size: usize,
    vector: Vec<T>,
}

impl<T> EntryVector<T> {
    pub fn new(entry_size: usize) -> Self {
        Self {
            entry_size,
            vector: vec![],
        }
    }

    /// Inserts a single entry of entry_size values from an iterator
    /// # Safety
    /// Make sure the entry has entry_size values
    #[inline]
    pub unsafe fn push_unchecked(&mut self, iter: impl IntoIterator<Item = T>) {
        let mut iter = iter.into_iter();
        self.vector.extend(iter.by_ref().take(self.entry_size));
    }

    /// Inserts a single entry of entry_size values from an iterator
    /// It will panic if the iterator doesn't have the correct size
    #[inline]
    pub fn push(&mut self, iter: impl IntoIterator<Item = T>) {
        let mut iter = iter.into_iter();
        unsafe { self.push_unchecked(iter.by_ref()) };
        assert!(iter.next().is_none());
    }

    /// Returns the number of entries that are contained
    #[inline]
    pub fn len(&self) -> usize {
        self.vector.len() / self.entry_size
    }

    /// Returns true if it contains no elements
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.vector.is_empty()
    }
}

impl<T> Index<usize> for EntryVector<T> {
    type Output = [T];
    fn index(&self, index: usize) -> &Self::Output {
        let internal_index = index * self.entry_size;
        assert!(
            internal_index + self.entry_size - 1 < self.vector.len(),
            "Out of bounds access"
        );
        &self.vector[internal_index..(internal_index + self.entry_size)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry() {
        let mut v = EntryVector::<usize>::new(5);
        let entry = [1, 2, 3, 4, 5];
        v.push(entry);
        assert_eq!(v[0], entry);
    }

    #[test]
    #[should_panic]
    fn test_incorrect_size() {
        let mut v = EntryVector::<usize>::new(5);
        let entry = [1, 2, 3, 4, 5, 6];
        v.push(entry);
    }

    #[test]
    #[should_panic]
    fn test_out_of_bounds() {
        let mut v = EntryVector::<usize>::new(5);
        let entry = [1, 2, 3, 4, 5];
        v.push(entry);
        let _entry = &v[1];
    }
}
