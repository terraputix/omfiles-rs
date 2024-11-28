use crate::aligned_buffer::{as_bytes, as_typed_slice_mut, AlignToSixtyFour};
use crate::compression::{p4nenc256_bound, CompressionType};
use crate::delta2d::{delta2d_encode, delta2d_encode_xor};
use crate::om::backends::{InMemoryBackend, OmFileWriterBackend};
use crate::om::dimensions::Dimensions;
use crate::om::errors::OmFilesRsError;
use crate::om::header::OmHeader;
use crate::utils::divide_rounded_up;
use std::fs::File;
use std::path::Path;
use std::rc::Rc;
// use turbo_pfor_sys::{fpxenc32, p4nzenc128v16};
use omfileformatc_rs::{fpxenc32, p4nzenc128v16};

/// Writer for OM files.
/// The format currently looks like this:
/// - 2 bytes magic number: 0x4F4D (79 77) "OM"
/// - 1 byte version: 2
/// - 1 byte compression type with filter
/// - 4 bytes scale factor
/// - 8 bytes dim0 (slow)
/// - 8 bytes dim1 (fast)
/// - 8 bytes chunk dim0
/// - 8 bytes chunk dim1
/// - Reserve space for reference table
/// - Data blocks
pub struct OmFileWriter {
    pub dim0: usize,
    pub dim1: usize,
    pub chunk0: usize,
    pub chunk1: usize,
}

impl OmFileWriter {
    pub fn new(dim0: usize, dim1: usize, chunk0: usize, chunk1: usize) -> Self {
        Self {
            dim0,
            dim1,
            chunk0,
            chunk1,
        }
    }

    /// Write new or overwrite new compressed file.
    /// Data must be supplied by a closure which returns the current position in dimension 0.
    /// Typically this is the location offset.
    /// The closure must return either an even number of elements of chunk1 * dim1 or all the
    /// remaining elements in the last chunk.
    ///
    /// One chunk should be around 2'000 or 16'000 elements. Fewer or more are not useful.
    /// If fsync is true, the file is synchronized after every 32 MB of data.
    ///
    /// Note: chunk0 can be an uneven multiple of dim0, e.g. for 10 locations we can use
    /// chunks of 3, so that the last chunk will only cover 1 location.
    pub fn write<'a, Backend: OmFileWriterBackend>(
        &self,
        backend: Backend,
        compression_type: CompressionType,
        scale_factor: f32,
        fsync: bool,
        supply_chunk: impl Fn(usize) -> Result<Rc<Vec<f32>>, OmFilesRsError>,
    ) -> Result<(), OmFilesRsError> {
        let mut state = OmFileWriterState::new(
            backend,
            self.dim0,
            self.dim1,
            self.chunk0,
            self.chunk1,
            compression_type,
            scale_factor,
            fsync,
        )?;

        state.write_header()?;
        while state.c0 < state.dimensions.n_dim0_chunks() {
            let uncompressed_input = supply_chunk(state.c0 * state.dimensions.chunk0)?;
            state.write(&uncompressed_input)?;
        }
        state.write_tail()?;

        Ok(())
    }

    pub fn write_to_file<'a>(
        &self,
        file: &str,
        compression_type: CompressionType,
        scale_factor: f32,
        overwrite: bool,
        supply_chunk: impl Fn(usize) -> Result<Rc<Vec<f32>>, OmFilesRsError>,
    ) -> Result<File, OmFilesRsError> {
        if !overwrite && Path::new(file).exists() {
            return Err(OmFilesRsError::FileExistsAlready {
                filename: file.to_string(),
            });
        }
        let file_temp = format!("{}~", file);
        if Path::new(&file_temp).exists() {
            std::fs::remove_file(&file_temp).map_err(|e| OmFilesRsError::CannotOpenFile {
                filename: file_temp.clone(),
                errno: e.raw_os_error().unwrap_or(0),
                error: e.to_string(),
            })?;
        }
        let file_handle = File::create(&file_temp).map_err(|e| OmFilesRsError::CannotOpenFile {
            filename: file_temp.clone(),
            errno: e.raw_os_error().unwrap_or(0),
            error: e.to_string(),
        })?;
        self.write(
            &file_handle,
            compression_type,
            scale_factor,
            true,
            supply_chunk,
        )?;
        std::fs::rename(&file_temp, file).map_err(|e| OmFilesRsError::CannotOpenFile {
            filename: file_temp,
            errno: e.raw_os_error().unwrap_or(0),
            error: e.to_string(),
        })?;
        Ok(file_handle)
    }

    pub fn write_all_to_file(
        &self,
        file: &str,
        compression_type: CompressionType,
        scale_factor: f32,
        all: Rc<Vec<f32>>,
        overwrite: bool,
    ) -> Result<File, OmFilesRsError> {
        self.write_to_file(file, compression_type, scale_factor, overwrite, |_| {
            Ok(all.clone())
        })
    }

    pub fn write_in_memory<'a>(
        &self,
        compression_type: CompressionType,
        scale_factor: f32,
        supply_chunk: impl Fn(usize) -> Result<Rc<Vec<f32>>, OmFilesRsError>,
    ) -> Result<InMemoryBackend, OmFilesRsError> {
        let mut data = InMemoryBackend::new(Vec::new());
        self.write(
            &mut data,
            compression_type,
            scale_factor,
            true,
            supply_chunk,
        )?;
        Ok(data)
    }

    pub fn write_all_in_memory(
        &self,
        compression_type: CompressionType,
        scale_factor: f32,
        all: Rc<Vec<f32>>,
    ) -> Result<InMemoryBackend, OmFilesRsError> {
        self.write_in_memory(compression_type, scale_factor, |_| Ok(all.clone()))
    }
}

pub struct OmFileWriterState<Backend: OmFileWriterBackend> {
    pub backend: Backend,

    pub dimensions: Dimensions,

    pub compression: CompressionType,
    pub scale_factor: f32,

    /// Buffer where chunks are moved to, before compressing them.
    /// -> Input for compression call
    read_buffer: AlignToSixtyFour,

    /// Compressed chunks are written into this buffer.
    /// 1 MB write buffer or larger if chunks are very large.
    write_buffer: AlignToSixtyFour,

    pub bytes_written_since_last_flush: usize,

    write_buffer_pos: usize,

    /// Number of bytes after data should be flushed with fsync
    pub fsync_flush_size: Option<usize>,

    /// Position of last written chunk in dimension 0
    c0: usize,

    /// Stores all byte offsets where our compressed chunks start.
    /// Later, we want to decompress chunk 1234 and know it starts at
    /// byte offset 5346545.
    chunk_offset_bytes: Vec<u64>,
}

impl<Backend: OmFileWriterBackend> OmFileWriterState<Backend> {
    pub fn new(
        backend: Backend,
        dim0: usize,
        dim1: usize,
        chunk0: usize,
        chunk1: usize,
        compression: CompressionType,
        scale_factor: f32,
        fsync: bool,
    ) -> Result<Self, OmFilesRsError> {
        if chunk0 == 0 || chunk1 == 0 || dim0 == 0 || dim1 == 0 {
            return Err(OmFilesRsError::DimensionMustBeLargerThan0);
        }
        if chunk0 > dim0 || chunk1 > dim1 {
            return Err(OmFilesRsError::ChunkDimensionIsSmallerThanOverallDim);
        }

        let chunk_size_byte = chunk0 * chunk1 * 4;
        if chunk_size_byte > 1024 * 1024 * 4 {
            println!(
                "WARNING: Chunk size greater than 4 MB ({} MB)!",
                chunk_size_byte as f32 / 1024.0 / 1024.0
            );
        }

        let buffer_size = p4nenc256_bound(chunk0 * chunk1, 4);

        let dimensions = Dimensions::new(dim0, dim1, chunk0, chunk1);
        let chunk_offset_length = dimensions.chunk_offset_length();
        Ok(Self {
            backend,
            dimensions,
            compression,
            scale_factor,
            read_buffer: AlignToSixtyFour::new(buffer_size as usize),
            write_buffer: AlignToSixtyFour::new(std::cmp::max(1024 * 1024, buffer_size as usize)),
            bytes_written_since_last_flush: 0,
            write_buffer_pos: 0,
            fsync_flush_size: if fsync { Some(32 * 1024 * 1024) } else { None },
            c0: 0,
            chunk_offset_bytes: Vec::with_capacity(chunk_offset_length as usize),
        })
    }

    pub fn write_header(&mut self) -> Result<(), OmFilesRsError> {
        let header = OmHeader {
            magic_number1: OmHeader::MAGIC_NUMBER1,
            magic_number2: OmHeader::MAGIC_NUMBER2,
            version: OmHeader::VERSION,
            compression: self.compression,
            scale_factor: self.scale_factor,
            dim0: self.dimensions.dim0 as u64,
            dim1: self.dimensions.dim1 as u64,
            chunk0: self.dimensions.chunk0 as u64,
            chunk1: self.dimensions.chunk1 as u64,
        };

        // write the header to the file
        let header_bytes = header.as_bytes();
        self.backend.write(header_bytes.as_slice())?;

        // write empty chunk offset table
        // TODO: Wouldn't using usize make some problems if files are shared between 32 and 64 bit systems?
        let zero_bytes = vec![0; self.dimensions.chunk_offset_length() as usize];
        self.backend.write(&zero_bytes)?;

        Ok(())
    }

    pub fn write_tail(&mut self) -> Result<(), OmFilesRsError> {
        // write remaining data from buffer
        self.backend
            .write(&self.write_buffer[..self.write_buffer_pos as usize])?;

        // write trailing byte to allow the encoder to read with 256 bit alignment
        let trailing_bytes = p4nenc256_bound(0, 4);
        let trailing_data = vec![0; trailing_bytes as usize];
        self.backend.write(&trailing_data)?;

        let chunk_offset_bytes = as_bytes(self.chunk_offset_bytes.as_slice());

        // write chunk_offsets dictionary after the header,
        // we initially wrote zeros in these places!
        self.backend
            .write_at(chunk_offset_bytes, OmHeader::LENGTH)?;

        if let Some(_fsync_flush_size) = self.fsync_flush_size {
            self.backend.synchronize()?;
        }

        Ok(())
    }

    pub fn write(&mut self, uncompressed_input: &[f32]) -> Result<(), OmFilesRsError> {
        match self.compression {
            CompressionType::P4nzdec256 => {
                let scale_factor = self.scale_factor;
                self.write_compressed::<i16, _, _, _>(
                    uncompressed_input,
                    |val| {
                        if val.is_nan() {
                            i16::MAX
                        } else {
                            let scaled = val * scale_factor;
                            scaled.round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
                        }
                    },
                    delta2d_encode,
                    |a0, a1, a2| unsafe {
                        p4nzenc128v16(a0.as_mut_ptr() as *mut u16, a1, a2.as_mut_ptr())
                    },
                )
            }
            CompressionType::Fpxdec32 => self.write_compressed::<f32, _, _, _>(
                uncompressed_input,
                |val| *val,
                delta2d_encode_xor,
                |a0, a1, a2| unsafe {
                    fpxenc32(a0.as_mut_ptr() as *mut u32, a1, a2.as_mut_ptr(), 0)
                },
            ),
            CompressionType::P4nzdec256logarithmic => {
                let scale_factor = self.scale_factor;
                self.write_compressed::<i16, _, _, _>(
                    uncompressed_input,
                    |val| {
                        if val.is_nan() {
                            i16::MAX
                        } else {
                            let scaled = (val + 1.0).log10() * scale_factor;
                            scaled.round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
                        }
                    },
                    delta2d_encode,
                    |a0, a1, a2| unsafe {
                        p4nzenc128v16(a0.as_mut_ptr() as *mut u16, a1, a2.as_mut_ptr())
                    },
                )
            }
        }
    }

    pub fn write_compressed<T, F, G, H>(
        &mut self,
        uncompressed_input: &[f32],
        scaler_conversion: H,
        delta2d_encode_function: G,
        compression_function: F,
    ) -> Result<(), OmFilesRsError>
    where
        F: Fn(&mut [T], usize, &mut [u8]) -> usize,
        G: Fn(usize, usize, &mut [T]),
        H: Fn(&f32) -> T,
    {
        let read_buffer_length = self.read_buffer.len();
        let n_dim1_chunks = self.dimensions.n_dim1_chunks();

        let mut buffer = as_typed_slice_mut::<T, u8>(self.read_buffer.as_mut_slice());

        let elements_per_chunk_row = self.dimensions.elements_per_chunk_row();
        let missing_elements =
            self.dimensions.dim0 * self.dimensions.dim1 - self.c0 * elements_per_chunk_row;

        if missing_elements < elements_per_chunk_row {
            // For the last chunk, the number must match exactly
            if uncompressed_input.len() != missing_elements as usize {
                return Err(OmFilesRsError::ChunkHasWrongNumberOfElements);
            }
        }

        let is_even_multiple_of_chunk_size = uncompressed_input.len() % elements_per_chunk_row == 0;
        if !is_even_multiple_of_chunk_size && uncompressed_input.len() != missing_elements {
            return Err(OmFilesRsError::ChunkHasWrongNumberOfElements);
        }

        let n_read_chunks = divide_rounded_up(uncompressed_input.len(), elements_per_chunk_row);

        for c00 in 0..n_read_chunks {
            let length0 = std::cmp::min(
                (self.c0 + c00 + 1) * self.dimensions.chunk0,
                self.dimensions.dim0,
            ) - (self.c0 + c00) * self.dimensions.chunk0;

            for c1 in 0..n_dim1_chunks {
                // load chunk into buffer
                // consider the length, even if the last is only partial!
                // E.g. at 1000 elements with 600 chunk length, the last one is only 400
                let length1 =
                    std::cmp::min((c1 + 1) * self.dimensions.chunk1, self.dimensions.dim1)
                        - c1 * self.dimensions.chunk1;

                for d0 in 0..length0 {
                    // FIXME: elements_per_chunk_row potentially not correct here
                    let start = c1 * self.dimensions.chunk1
                        + d0 * self.dimensions.dim1
                        + c00 * elements_per_chunk_row; // FIXME: + uncompressedInput.startIndex ??
                    let range_buffer = d0 * length1..(d0 + 1) * length1;
                    let range_input = start..start + length1;

                    for (pos_buffer, pos_input) in range_buffer.zip(range_input) {
                        let val = uncompressed_input[pos_input as usize];
                        buffer[pos_buffer as usize] = scaler_conversion(&val);
                    }
                }
                delta2d_encode_function(length0, length1, &mut buffer);

                // encoding functions have the following form
                // size_t compressed_size = encode( unsigned *in, size_t n, char *out)
                // compressed_size : number of bytes written into compressed output buffer out
                let write_length = compression_function(
                    buffer,
                    (length1 * length0) as usize,
                    self.write_buffer[self.write_buffer_pos as usize..].as_mut(),
                );

                // If the write_buffer is too full, write it to the backend
                // Too full means, that the next compressed chunk may not fit into the buffer
                self.write_buffer_pos += write_length;
                if self.write_buffer.len() - (self.write_buffer_pos as usize) < read_buffer_length {
                    self.backend
                        .write(&self.write_buffer[..self.write_buffer_pos as usize])?;
                    if let Some(fsync_flush_size) = self.fsync_flush_size {
                        self.bytes_written_since_last_flush += self.write_buffer_pos;
                        if self.bytes_written_since_last_flush >= fsync_flush_size {
                            // Make sure to write to disk, otherwise we get a
                            // lot of dirty pages and might overload kernel page cache
                            self.backend.synchronize()?;
                            self.bytes_written_since_last_flush = 0;
                        }
                    }
                    self.write_buffer_pos = 0;
                }

                // Store chunk offset position in our lookup table
                let previous = *self.chunk_offset_bytes.last().unwrap_or(&0);
                self.chunk_offset_bytes.push(previous + write_length as u64);
            }
        }
        self.c0 += n_read_chunks;
        Ok(())
    }
}
