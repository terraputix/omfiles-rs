use crate::aligned_buffer::{as_typed_slice, as_typed_slice_mut};
use crate::compression::{p4ndec256_bound, CompressionType};
use crate::delta2d::{delta2d_decode, delta2d_decode_xor};
use crate::om::backends::OmFileReaderBackend;
use crate::om::dimensions::Dimensions;
use crate::om::errors::OmFilesRsError;
use crate::om::header::OmHeader;
use crate::om::mmapfile::{MmapFile, Mode};
use crate::utils::{add_range, clamp_range, divide_range, subtract_range};
use std::fs::File;
use std::ops::Range;
// use turbo_pfor_sys::{fpxdec32, p4nzdec128v16};
use omfileformatc_rs::{fpxdec32, p4nzdec128v16};

pub struct OmFileReader<Backend: OmFileReaderBackend> {
    pub backend: Backend,
    pub scalefactor: f32,
    pub compression: CompressionType,
    pub dimensions: Dimensions,
}

impl<Backend: OmFileReaderBackend> OmFileReader<Backend> {
    pub fn new(backend: Backend) -> Result<Self, OmFilesRsError> {
        // Fetch header
        backend.pre_read(0, OmHeader::LENGTH)?;
        let bytes = backend.get_bytes(0, OmHeader::LENGTH)?;
        let header = OmHeader::from_bytes(bytes)?;

        let dimensions = Dimensions::new(header.dim0, header.dim1, header.chunk0, header.chunk1);

        Ok(Self {
            backend,
            dimensions: dimensions,
            scalefactor: header.scalefactor,
            compression: if header.version == 1 {
                CompressionType::P4nzdec256
            } else {
                CompressionType::try_from(header.compression)?
            },
        })
    }

    pub fn will_need(
        &mut self,
        dim0_read: Option<Range<usize>>,
        dim1_read: Option<Range<usize>>,
    ) -> Result<(), OmFilesRsError> {
        if !self.backend.needs_prefetch() {
            return Ok(());
        }

        let dim0_read = dim0_read.unwrap_or(0..self.dimensions.dim0);
        let dim1_read = dim1_read.unwrap_or(0..self.dimensions.dim1);

        // verify that the read ranges are within the dimensions
        self.dimensions.check_read_ranges(&dim0_read, &dim1_read)?;

        // Fetch chunk table
        let chunk_table_buffer = self.backend.get_bytes(
            OmHeader::LENGTH,
            self.dimensions.chunk_offset_length() + OmHeader::LENGTH,
        )?;
        let chunk_offsets = as_typed_slice::<usize>(chunk_table_buffer);

        // let n_dim0_chunks = self.dimensions.n_dim0_chunks();
        let n_dim1_chunks = self.dimensions.n_dim1_chunks();
        let n_chunks = self.dimensions.n_chunks();

        let mut fetch_start = 0;
        let mut fetch_end = 0;

        let compressed_data_start_offset =
            OmHeader::LENGTH + n_chunks * std::mem::size_of::<usize>();

        for c0 in divide_range(&dim0_read, self.dimensions.chunk0) {
            let c1_range = divide_range(&dim1_read, self.dimensions.chunk1);
            let c1_chunks = add_range(&c1_range, c0 * n_dim1_chunks);

            self.backend.prefetch_data(
                OmHeader::LENGTH
                    + std::cmp::max(c1_chunks.start as isize - 1, 0) as usize
                        * std::mem::size_of::<usize>(),
                (c1_range.len() + 1) * std::mem::size_of::<usize>(),
            );

            for c1 in c1_range {
                let chunk_num = c0 * n_dim1_chunks + c1;
                let start_pos = if chunk_num == 0 {
                    0
                } else {
                    chunk_offsets[chunk_num - 1]
                };
                let length_compressed_bytes = chunk_offsets[chunk_num] - start_pos;

                let new_fetch_start = compressed_data_start_offset + start_pos;
                let new_fetch_end = new_fetch_start + length_compressed_bytes;

                if new_fetch_start != fetch_end {
                    if fetch_end != 0 {
                        self.backend
                            .prefetch_data(fetch_start, fetch_end - fetch_start);
                    }
                    fetch_start = new_fetch_start;
                }
                fetch_end = new_fetch_end;
            }
        }

        self.backend
            .prefetch_data(fetch_start, fetch_end - fetch_start);
        Ok(())
    }

    pub fn read(
        &self,
        into: &mut [f32],
        array_dim1_range: Range<usize>,
        array_dim1_length: usize,
        chunk_buffer: &mut [u8],
        dim0_read: Range<usize>,
        dim1_read: Range<usize>,
    ) -> Result<(), OmFilesRsError> {
        match self.compression {
            CompressionType::P4nzdec256 => {
                let chunk_buffer = as_typed_slice_mut::<i16, u8>(chunk_buffer);
                self.read_compressed(
                    into,
                    array_dim1_range,
                    array_dim1_length,
                    chunk_buffer,
                    dim0_read,
                    dim1_read,
                    |a0, a1, a2| unsafe {
                        p4nzdec128v16(a0.as_ptr() as *mut u8, a1, a2.as_mut_ptr() as *mut u16)
                    },
                    delta2d_decode,
                    |val| {
                        if val == i16::MAX {
                            f32::NAN
                        } else {
                            val as f32 / self.scalefactor
                        }
                    },
                )
            }
            CompressionType::Fpxdec32 => {
                let chunk_buffer = as_typed_slice_mut::<f32, u8>(chunk_buffer);
                self.read_compressed(
                    into,
                    array_dim1_range,
                    array_dim1_length,
                    chunk_buffer,
                    dim0_read,
                    dim1_read,
                    |a0, a1, a2| unsafe {
                        fpxdec32(a0.as_ptr() as *mut u8, a1, a2.as_mut_ptr() as *mut u32, 0)
                    },
                    delta2d_decode_xor,
                    |val| val,
                )
            }
            CompressionType::P4nzdec256logarithmic => {
                let chunk_buffer = as_typed_slice_mut::<i16, u8>(chunk_buffer);
                self.read_compressed(
                    into,
                    array_dim1_range,
                    array_dim1_length,
                    chunk_buffer,
                    dim0_read,
                    dim1_read,
                    |a0, a1, a2| unsafe {
                        p4nzdec128v16(a0.as_ptr() as *mut u8, a1, a2.as_mut_ptr() as *mut u16)
                    },
                    delta2d_decode,
                    |val| {
                        if val == i16::MAX {
                            f32::NAN
                        } else {
                            10f32.powf(val as f32 / self.scalefactor) - 1.0
                        }
                    },
                )
            }
        }
    }

    #[inline(always)]
    pub fn read_compressed<T, F, G, H>(
        &self,
        into: &mut [f32],
        array_dim1_range: Range<usize>,
        array_dim1_length: usize,
        chunk_buffer: &mut [T],
        dim0_read: Range<usize>,
        dim1_read: Range<usize>,
        decompression_function: F,
        delta_decoding_function: G,
        into_conversion: H,
    ) -> Result<(), OmFilesRsError>
    where
        T: Copy + Clone,
        F: Fn(&[u8], usize, &mut [T]) -> usize,
        G: Fn(usize, usize, &mut [T]),
        H: Fn(T) -> f32,
    {
        self.dimensions.check_read_ranges(&dim0_read, &dim1_read)?;
        let buffer = self.backend.get_bytes(0, self.backend.count())?;
        let compressed_data_start_offset = OmHeader::LENGTH + self.dimensions.chunk_offset_length();

        // Fetch chunk table
        let chunk_offsets =
            as_typed_slice::<usize>(&buffer[OmHeader::LENGTH..compressed_data_start_offset]);

        // let n_dim0_chunks = self.dimensions.n_dim0_chunks();
        let n_dim1_chunks = self.dimensions.n_dim1_chunks();
        let n_chunks = self.dimensions.n_chunks();

        let compressed_data_buffer = &buffer[compressed_data_start_offset..];

        for c0 in divide_range(&dim0_read, self.dimensions.chunk0) {
            let c1_range = divide_range(&dim1_read, self.dimensions.chunk1);
            let c1_chunks = add_range(&c1_range, c0 * n_dim1_chunks);

            self.backend.pre_read(
                OmHeader::LENGTH
                    + std::cmp::max(c1_chunks.start as isize - 1, 0) as usize
                        * std::mem::size_of::<usize>(),
                (c1_range.len() + 1) * std::mem::size_of::<usize>(),
            )?;

            for c1 in c1_range {
                let length1 =
                    std::cmp::min((c1 + 1) * self.dimensions.chunk1, self.dimensions.dim1)
                        - c1 * self.dimensions.chunk1;
                let length0 =
                    std::cmp::min((c0 + 1) * self.dimensions.chunk0, self.dimensions.dim0)
                        - c0 * self.dimensions.chunk0;

                let chunk_global0 =
                    c0 * self.dimensions.chunk0..c0 * self.dimensions.chunk0 + length0;
                let chunk_global1 =
                    c1 * self.dimensions.chunk1..c1 * self.dimensions.chunk1 + length1;

                let clamped_global0 = clamp_range(&chunk_global0, &dim0_read);
                let clamped_global1 = clamp_range(&chunk_global1, &dim1_read);

                let chunk_num = c0 * n_dim1_chunks + c1;
                assert!(chunk_num < n_chunks, "Chunk number out of bounds");
                let start_pos = if chunk_num == 0 {
                    0
                } else {
                    chunk_offsets[chunk_num - 1]
                };
                assert!(
                    compressed_data_start_offset + start_pos < self.backend.count(),
                    "Chunk out of range read"
                );
                let length_compressed_bytes = chunk_offsets[chunk_num] - start_pos;

                self.backend.pre_read(
                    compressed_data_start_offset + start_pos,
                    length_compressed_bytes,
                )?;

                // decompression is a function like
                // size_t compressed_size = decode( char *in, size_t n, unsigned *out)
                // compressed_size : number of bytes read from compressed input buffer in
                let compressed_bytes = decompression_function(
                    &compressed_data_buffer[start_pos..start_pos + length_compressed_bytes],
                    length0 * length1,
                    chunk_buffer,
                );

                assert_eq!(
                    compressed_bytes, length_compressed_bytes,
                    "chunk read bytes mismatch"
                );

                delta_decoding_function(length0, length1, chunk_buffer);

                let clamped_local0 = subtract_range(&clamped_global0, c0 * self.dimensions.chunk0);
                let clamped_local1 = clamped_global1.start - c1 * self.dimensions.chunk1;

                for d0 in clamped_local0 {
                    let read_start = clamped_local1 + d0 * length1;
                    let local_out0 = chunk_global0.start + d0 - dim0_read.start;
                    let local_out1 = clamped_global1.start - dim1_read.start;
                    let local_range =
                        local_out1 + local_out0 * array_dim1_length + array_dim1_range.start;

                    for i in 0..clamped_global1.len() {
                        let pos_buffer = read_start + i;
                        let pos_out = local_range + i;
                        let val = chunk_buffer[pos_buffer];
                        into[pos_out] = into_conversion(val);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn read_all(&mut self) -> Result<Vec<f32>, OmFilesRsError> {
        // Prefetch data
        self.backend.prefetch_data(0, self.backend.count());

        // Create a buffer to hold the data
        let mut buffer = vec![0.0; self.dimensions.dim0 * self.dimensions.dim1];

        // Read the data into the buffer
        self.read(
            &mut buffer,
            0..self.dimensions.dim1,
            self.dimensions.dim1,
            vec![
                0;
                p4ndec256_bound(
                    self.dimensions.chunk0 * self.dimensions.chunk1,
                    self.compression.bytes_per_element()
                )
            ]
            .as_mut_slice(),
            0..self.dimensions.dim0,
            0..self.dimensions.dim1,
        )?;

        Ok(buffer)
    }
}

impl OmFileReader<MmapFile> {
    /// Convenience initializer to create an `OmFileReader` from a file path.
    pub fn from_file(file: &str) -> Result<Self, OmFilesRsError> {
        let file_handle = File::open(file).map_err(|e| OmFilesRsError::CannotOpenFile {
            filename: file.to_string(),
            errno: e.raw_os_error().unwrap_or(0),
            error: e.to_string(),
        })?;
        Self::from_file_handle(file_handle)
    }

    /// Convenience initializer to create an `OmFileReader` from an existing `FileHandle`.
    pub fn from_file_handle(file_handle: File) -> Result<Self, OmFilesRsError> {
        // TODO: Error handling
        let mmap = MmapFile::new(file_handle, Mode::ReadOnly).unwrap();
        Self::new(mmap)
    }

    /// Check if the file was deleted on the file system.
    /// Linux keeps the file alive as long as some processes have it open.
    pub fn was_deleted(&self) -> bool {
        self.backend.was_deleted()
    }
}

impl<Backend: OmFileReaderBackend> OmFileReader<Backend> {
    /// Read data. This version is a bit slower, because it is allocating the output buffer
    pub fn read_range(
        &self,
        dim0_read: Option<Range<usize>>,
        dim1_read: Option<Range<usize>>,
    ) -> Result<Vec<f32>, OmFilesRsError> {
        // Handle default ranges
        let dim0_read = dim0_read.unwrap_or(0..self.dimensions.dim0);
        let dim1_read = dim1_read.unwrap_or(0..self.dimensions.dim1);

        // Calculate the count
        let count = dim0_read.len() * dim1_read.len();

        let mut buffer = vec![0.0; count];
        let slice = buffer.as_mut_slice();

        // Read data into the buffer
        self.read_into(
            slice,
            0..dim1_read.len(),
            dim1_read.len(),
            dim0_read,
            dim1_read,
        )?;

        Ok(buffer)
    }

    // Read data into existing output float buffer
    pub fn read_into(
        &self,
        into: &mut [f32],
        array_dim1_range: Range<usize>,
        array_dim1_length: usize,
        dim0_read: Range<usize>,
        dim1_read: Range<usize>,
    ) -> Result<(), OmFilesRsError> {
        // assert!(array_dim1_range.len() == dim1_read.len());
        let mut chunk_buffer = vec![
            0;
            p4ndec256_bound(
                self.dimensions.chunk0 * self.dimensions.chunk1,
                self.compression.bytes_per_element()
            )
        ];
        self.read(
            into,
            array_dim1_range,
            array_dim1_length,
            chunk_buffer.as_mut_slice(),
            dim0_read,
            dim1_read,
        )
    }
}
