use crate::aligned_buffer::as_typed_slice;
use crate::compression::{p4ndec256_bound, CompressionType};
use crate::om::backends::OmFileReaderBackend;
use crate::om::dimensions::Dimensions;
use crate::om::errors::OmFilesRsError;
use crate::om::header::OmHeader;
use crate::om::io::mmapfile::{MmapFile, Mode};
use crate::utils::{add_range, divide_range};
use omfileformatc_rs::{OmCompression_t, OmDataType_t_DATA_TYPE_FLOAT, OmDecoder_init};
use std::fs::File;
use std::ops::Range;

use super::c_defaults::create_decoder;

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
            dimensions,
            scalefactor: header.scalefactor,
            compression: if header.version == 1 {
                CompressionType::P4nzdec256
            } else {
                header.compression
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
        self.dimensions.check_read_ranges(&dim0_read, &dim1_read)?;

        let mut ptr = vec![0usize; 12];

        // dimensions
        ptr[0] = self.dimensions.dim0 as usize;
        ptr[1] = self.dimensions.dim1 as usize;
        // chunks
        ptr[2] = self.dimensions.chunk0 as usize;
        ptr[3] = self.dimensions.chunk1 as usize;
        // read offset
        ptr[4] = dim0_read.start as usize;
        ptr[5] = dim1_read.start as usize;
        // read count
        ptr[6] = dim0_read.len() as usize;
        ptr[7] = dim1_read.len() as usize;
        // cube offset
        ptr[8] = 0;
        ptr[9] = array_dim1_range.start as usize;
        // cube dimensions
        ptr[10] = dim0_read.len() as usize;
        ptr[11] = array_dim1_length as usize;

        let ptr = ptr.as_mut_ptr();

        let mut decoder = create_decoder();
        unsafe {
            OmDecoder_init(
                &mut decoder,
                self.scalefactor,
                0.0, // add_offset
                self.compression as OmCompression_t,
                OmDataType_t_DATA_TYPE_FLOAT,
                2,
                ptr,
                ptr.add(2),
                ptr.add(4),
                ptr.add(6),
                ptr.add(8),
                ptr.add(10),
                8,
                1,
                OmHeader::LENGTH,
                512,
                65536,
            );
            self.backend.decode(&mut decoder, into, chunk_buffer)?;
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
