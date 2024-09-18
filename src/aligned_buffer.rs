use std::mem;
use std::ops::{Deref, DerefMut};

#[repr(C, align(64))]
pub struct AlignToSixtyFour(Vec<u8>);

impl AlignToSixtyFour {
    pub fn new(length_u8: usize) -> Self {
        let aligned = vec![0; length_u8];

        AlignToSixtyFour(aligned)
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl Deref for AlignToSixtyFour {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for AlignToSixtyFour {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub fn as_typed_slice<T>(v: &[u8]) -> &[T] {
    assert_eq!(v.len() % mem::size_of::<T>(), 0);
    assert_eq!(v.as_ptr() as usize % std::mem::align_of::<T>(), 0);
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const T, v.len() / mem::size_of::<T>()) }
}

pub fn as_typed_slice_mut<T, U>(v: &mut [U]) -> &mut [T] {
    let target_len = v.len() * mem::size_of::<U>() / mem::size_of::<T>();
    assert_eq!(v.len() % target_len, 0);
    let ptr = v.as_mut_ptr() as *mut T;
    assert_eq!(ptr as usize % std::mem::align_of::<T>(), 0);
    unsafe { std::slice::from_raw_parts_mut(ptr, target_len) }
}

pub fn as_bytes<T>(v: &[T]) -> &[u8] {
    let len = v.len() * mem::size_of::<T>();
    let ptr = v.as_ptr() as *const u8;
    unsafe { std::slice::from_raw_parts(ptr, len) }
}
