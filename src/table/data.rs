use crate::table::metadata::Layout;

#[repr(transparent)]
#[derive(Debug)]
pub struct Data([u8]);

impl Data {
    pub fn new_ref(data: &[u8]) -> &Self {
        let ptr = data as *const [u8] as *const Self;
        unsafe { &*ptr }
    }

    pub fn new_mut(data: &mut [u8]) -> &mut Self {
        let ptr = data as *mut [u8] as *mut Self;
        unsafe { &mut *ptr }
    }

    #[inline]
    pub fn read_all(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    pub fn read(&self, layout: Layout) -> &[u8] {
        let start = layout.offset;
        let end = start + layout.size.size;
        &self.0[start..end]
    }

    #[inline]
    pub fn write_all(&mut self, data: &[u8]) {
        debug_assert!(
            self.0.len() == data.len(),
            "Can't write to data with different size"
        );
        self.0.copy_from_slice(data);
    }

    #[inline]
    pub fn write(&mut self, layout: Layout, data: &[u8]) {
        debug_assert_eq!(
            layout.size.size,
            data.len(),
            "Can't write to data with different size"
        );
        let start = layout.offset;
        let end = start + layout.size.size;
        self.0[start..end].copy_from_slice(data);
    }

    #[inline]
    pub fn get_mut(&mut self, layout: Layout) -> &mut [u8] {
        let start = layout.offset;
        let end = start + layout.size.size;
        &mut self.0[start..end]
    }
}

#[cfg(test)]
mod tests {
    use crate::table::metadata::Size;

    use super::*;

    #[test]
    fn test_read() {
        let buf = [1, 2, 3, 4, 5, 6];
        let data = Data::new_ref(&buf);
        let layout = Layout {
            offset: 2,
            size: Size {
                size: 2,
                aligned: 4,
            },
        };
        assert_eq!(data.read(layout), [3, 4])
    }

    #[test]
    fn test_write() {
        let mut buf = [1, 2, 3, 4, 5, 6];
        let data = Data::new_mut(&mut buf);
        let layout = Layout {
            offset: 2,
            size: Size {
                size: 2,
                aligned: 4,
            },
        };
        data.write(layout, &[8, 9]);
        assert_eq!(data.0, [1, 2, 8, 9, 5, 6])
    }
}
