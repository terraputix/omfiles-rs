use crate::aligned_buffer::as_typed_slice;
use crate::compression::{self, p4ndec256_bound, CompressionType};
use crate::om::backends::OmFileReaderBackend;
use crate::om::dimensions::Dimensions;
use crate::om::errors::OmFilesRsError;
use crate::om::header::OmHeader;
use crate::om::mmapfile::{MmapFile, Mode};
use crate::utils::{add_range, divide_range};
use omfileformatc_rs::{
    om_decoder_init, OmCompression_t, OmDataType_t_DATA_TYPE_FLOAT, OmError_t_ERROR_OK,
};
use std::fs::File;
use std::ops::Range;

use super::c_defaults::create_decoder;
use super::reader2::OmFileReader2;

pub struct OmFileReader<Backend: OmFileReaderBackend> {
    pub reader: OmFileReader2<Backend>,
    // pub backend: Backend,
    pub scalefactor: f32,
    pub compression: CompressionType,
    pub dimensions: Dimensions,
}

impl<Backend: OmFileReaderBackend> OmFileReader<Backend> {
    pub fn new(backend: Backend) -> Result<Self, OmFilesRsError> {
        let reader = OmFileReader2::new(backend, 256);

        let dimensions = reader.get_dimensions();
        let chunks = reader.get_chunk_dimensions();

        let dimensions = Dimensions::new(
            dimensions[0] as usize,
            dimensions[1] as usize,
            chunks[0] as usize,
            chunks[1] as usize,
        );
        let scale_factor = reader.scale_factor();
        let compression = reader.compression();

        Ok(Self {
            reader,
            dimensions,
            scalefactor: scale_factor,
            compression: compression,
        })
    }

    pub fn will_need(
        &mut self,
        dim0_read: Option<Range<usize>>,
        dim1_read: Option<Range<usize>>,
    ) -> Result<(), OmFilesRsError> {
        if !self.reader.backend.needs_prefetch() {
            return Ok(());
        }

        let dim0_read = dim0_read.unwrap_or(0..self.dimensions.dim0);
        let dim1_read = dim1_read.unwrap_or(0..self.dimensions.dim1);

        // verify that the read ranges are within the dimensions
        self.dimensions.check_read_ranges(&dim0_read, &dim1_read)?;

        // Fetch chunk table
        let chunk_table_buffer = self.reader.backend.get_bytes(
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

            self.reader.backend.prefetch_data(
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
                        self.reader
                            .backend
                            .prefetch_data(fetch_start, fetch_end - fetch_start);
                    }
                    fetch_start = new_fetch_start;
                }
                fetch_end = new_fetch_end;
            }
        }

        self.reader
            .backend
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

        let mut ptr = vec![0u64; 8];

        // read offset
        ptr[0] = dim0_read.start as u64;
        ptr[1] = dim1_read.start as u64;
        // read count
        ptr[2] = dim0_read.len() as u64;
        ptr[3] = dim1_read.len() as u64;
        // cube offset
        ptr[4] = 0;
        ptr[5] = array_dim1_range.start as u64;
        // cube dimensions
        ptr[6] = dim0_read.len() as u64;
        ptr[7] = array_dim1_length as u64;

        let ptr = ptr.as_mut_ptr();

        let mut decoder = create_decoder();
        let error = unsafe {
            om_decoder_init(
                &mut decoder,
                self.reader.variable,
                2,
                ptr,
                ptr.add(2),
                ptr.add(4),
                ptr.add(6),
                self.reader.lut_chunk_element_count as u64,
                512,
                65536 * 4,
            )
        };
        if error != OmError_t_ERROR_OK {
            panic!("Error initializing decoder");
        }
        self.reader
            .backend
            .decode(&mut decoder, into, chunk_buffer)?;

        Ok(())
    }

    pub fn read_all(&mut self) -> Result<Vec<f32>, OmFilesRsError> {
        // Prefetch data
        self.reader
            .backend
            .prefetch_data(0, self.reader.backend.count());

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
        self.reader.backend.was_deleted()
    }
}
