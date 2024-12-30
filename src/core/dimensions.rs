use crate::errors::OmFilesRsError;
use crate::utils::divide_rounded_up;
use std::ops::Range;

pub struct Dimensions {
    /// The slow dimension of the data.
    pub dim0: usize,
    /// The fast dimension of the data.
    pub dim1: usize,
    pub chunk0: usize,
    pub chunk1: usize,
}

impl Dimensions {
    pub fn new(dim0: usize, dim1: usize, chunk0: usize, chunk1: usize) -> Self {
        Self {
            dim0,
            dim1,
            chunk0,
            chunk1,
        }
    }

    pub fn n_dim0_chunks(&self) -> usize {
        divide_rounded_up(self.dim0, self.chunk0)
    }

    pub fn n_dim1_chunks(&self) -> usize {
        divide_rounded_up(self.dim1, self.chunk1)
    }

    pub fn n_chunks(&self) -> usize {
        self.n_dim0_chunks() * self.n_dim1_chunks()
    }

    pub fn elements_per_chunk_row(&self) -> usize {
        self.chunk0 * self.dim1
    }

    // length of the chunk offset table in bytes
    pub fn chunk_offset_length(&self) -> u64 {
        (self.n_chunks() * std::mem::size_of::<u64>()) as u64
    }

    #[inline(always)]
    pub fn check_read_ranges(
        &self,
        dim0_read: &Range<usize>,
        dim1_read: &Range<usize>,
    ) -> Result<(), OmFilesRsError> {
        if dim0_read.start > self.dim0 || dim0_read.end > self.dim0 {
            return Err(OmFilesRsError::DimensionOutOfBounds {
                range: dim0_read.clone(),
                allowed: self.dim0,
            });
        }
        if dim1_read.start > self.dim1 || dim1_read.end > self.dim1 {
            return Err(OmFilesRsError::DimensionOutOfBounds {
                range: dim1_read.clone(),
                allowed: self.dim1,
            });
        }

        Ok(())
    }
}
