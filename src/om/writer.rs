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
        scalefactor: f32,
        fsync: bool,
        supply_chunk: impl Fn(usize) -> Result<&'a [f32], OmFilesRsError>,
    ) -> Result<(), OmFilesRsError> {
        let mut state = OmFileWriterState::new(
            backend,
            self.dim0,
            self.dim1,
            self.chunk0,
            self.chunk1,
            compression_type,
            scalefactor,
            fsync,
        )?;

        state.write_header()?;
        while state.c0 < state.dimensions.n_dim0_chunks() {
            let uncompressed_input = supply_chunk(state.c0 * state.dimensions.chunk0)?;
            state.write(uncompressed_input)?;
        }
        state.write_tail()?;

        Ok(())
    }

    pub fn write_to_file<'a>(
        &self,
        file: &str,
        compression_type: CompressionType,
        scalefactor: f32,
        overwrite: bool,
        supply_chunk: impl Fn(usize) -> Result<&'a [f32], OmFilesRsError>,
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
        let mut file_handle =
            File::create(&file_temp).map_err(|e| OmFilesRsError::CannotOpenFile {
                filename: file_temp.clone(),
                errno: e.raw_os_error().unwrap_or(0),
                error: e.to_string(),
            })?;
        self.write(
            &mut file_handle,
            compression_type,
            scalefactor,
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
        scalefactor: f32,
        all: &[f32],
        overwrite: bool,
    ) -> Result<File, OmFilesRsError> {
        self.write_to_file(file, compression_type, scalefactor, overwrite, |_| Ok(all))
    }

    pub fn write_in_memory<'a>(
        &self,
        compression_type: CompressionType,
        scalefactor: f32,
        supply_chunk: impl Fn(usize) -> Result<&'a [f32], OmFilesRsError>,
    ) -> Result<InMemoryBackend, OmFilesRsError> {
        let mut data = InMemoryBackend::new(Vec::new());
        self.write(&mut data, compression_type, scalefactor, true, supply_chunk)?;
        Ok(data)
    }

    pub fn write_all_in_memory(
        &self,
        compression_type: CompressionType,
        scalefactor: f32,
        all: &Vec<f32>,
    ) -> Result<InMemoryBackend, OmFilesRsError> {
        self.write_in_memory(compression_type, scalefactor, |_| Ok(&all))
    }
}

pub struct OmFileWriterState<Backend: OmFileWriterBackend> {
    pub backend: Backend,

    pub dimensions: Dimensions,

    pub compression: CompressionType,
    pub scalefactor: f32,

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
    chunk_offset_bytes: Vec<usize>,
}

impl<Backend: OmFileWriterBackend> OmFileWriterState<Backend> {
    pub fn new(
        backend: Backend,
        dim0: usize,
        dim1: usize,
        chunk0: usize,
        chunk1: usize,
        compression: CompressionType,
        scalefactor: f32,
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
            scalefactor,
            read_buffer: AlignToSixtyFour::new(buffer_size),
            write_buffer: AlignToSixtyFour::new(std::cmp::max(1024 * 1024, buffer_size)),
            bytes_written_since_last_flush: 0,
            write_buffer_pos: 0,
            fsync_flush_size: if fsync { Some(32 * 1024 * 1024) } else { None },
            c0: 0,
            chunk_offset_bytes: Vec::with_capacity(chunk_offset_length),
        })
    }

    pub fn write_header(&mut self) -> Result<(), OmFilesRsError> {
        let header = OmHeader {
            magic_number1: OmHeader::MAGIC_NUMBER1,
            magic_number2: OmHeader::MAGIC_NUMBER2,
            version: OmHeader::VERSION,
            compression: self.compression as u8,
            scalefactor: self.scalefactor,
            dim0: self.dimensions.dim0,
            dim1: self.dimensions.dim1,
            chunk0: self.dimensions.chunk0,
            chunk1: self.dimensions.chunk1,
        };

        // write the header to the file
        let header_bytes = header.as_bytes();
        self.backend.write(header_bytes.as_slice())?;

        // write empty chunk offset table
        // TODO: Wouldn't using usize make some problems if files are shared between 32 and 64 bit systems?
        let zero_bytes = vec![0; self.dimensions.chunk_offset_length()];
        self.backend.write(&zero_bytes)?;

        Ok(())
    }

    pub fn write_tail(&mut self) -> Result<(), OmFilesRsError> {
        // write remaining data from buffer
        self.backend
            .write(&self.write_buffer[..self.write_buffer_pos])?;

        // write trailing byte to allow the encoder to read with 256 bit alignment
        let trailing_bytes = p4nenc256_bound(0, 4);
        let trailing_data = vec![0; trailing_bytes];
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
                let scalefactor = self.scalefactor;
                self.write_compressed::<i16, _, _, _>(
                    uncompressed_input,
                    |val| {
                        if val.is_nan() {
                            i16::MAX
                        } else {
                            let scaled = val * scalefactor;
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
                let scalefactor = self.scalefactor;
                self.write_compressed::<i16, _, _, _>(
                    uncompressed_input,
                    |val| {
                        if val.is_nan() {
                            i16::MAX
                        } else {
                            let scaled = (val.log10() + 1.0) * scalefactor;
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
            if uncompressed_input.len() != missing_elements {
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
                        let val = uncompressed_input[pos_input];
                        buffer[pos_buffer] = scaler_conversion(&val);
                    }
                }
                delta2d_encode_function(length0, length1, &mut buffer);

                // encoding functions have the following form
                // size_t compressed_size = encode( unsigned *in, size_t n, char *out)
                // compressed_size : number of bytes written into compressed output buffer out
                let write_length = compression_function(
                    buffer,
                    length1 * length0,
                    self.write_buffer[self.write_buffer_pos..].as_mut(),
                );

                // If the write_buffer is too full, write it to the backend
                // Too full means, that the next compressed chunk may not fit into the buffer
                self.write_buffer_pos += write_length;
                if self.write_buffer.len() - self.write_buffer_pos < read_buffer_length {
                    self.backend
                        .write(&self.write_buffer[..self.write_buffer_pos])?;
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
                self.chunk_offset_bytes.push(previous + write_length);
            }
        }
        self.c0 += n_read_chunks;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // use turbo_pfor_sys::fpxdec32;
    use omfileformatc_rs::{fpxdec32, om_datatype_t_DATA_TYPE_FLOAT};

    use crate::{
        compression::p4ndec256_bound,
        om::{
            backends::OmFileReaderBackend,
            encoder::{OmFileBufferedWriter, OmFileEncoder},
            mmapfile::{MmapFile, Mode},
            omfile_json::{OmFileJSON, OmFileJSONVariable},
            reader::OmFileReader,
            reader2::OmFileReader2,
        },
    };

    use super::*;
    use std::{borrow::BorrowMut, f32, fs, sync::Arc};

    #[test]
    fn turbo_pfor_roundtrip() {
        let data: Vec<f32> = vec![10.0, 22.0, 23.0, 24.0];
        let length = 1; //data.len();

        // create buffers for compression and decompression!
        let compressed_buffer = vec![0; p4nenc256_bound(length, 4)];
        let compressed = compressed_buffer.as_slice();
        let decompress_buffer = vec![0.0; p4ndec256_bound(length, 4)];
        let decompressed = decompress_buffer.as_slice();

        // compress data
        let compressed_size = unsafe {
            fpxenc32(
                data.as_ptr() as *mut u32,
                length,
                compressed.as_ptr() as *mut u8,
                0,
            )
        };
        if compressed_size >= compressed.len() {
            panic!("Compress Buffer too small");
        }

        // decompress data
        let decompressed_size = unsafe {
            fpxdec32(
                compressed.as_ptr() as *mut u8,
                length,
                decompressed.as_ptr() as *mut u32,
                0,
            )
        };
        if decompressed_size >= decompressed.len() {
            panic!("Decompress Buffer too small");
        }

        // this should be equal (we check it in the reader)
        // here we have a problem if length is only 1 and the exponent of the
        // float is greater than 0 (e.g. the value is greater than 10)
        // NOTE: This fails with 4 != 5
        assert_eq!(decompressed_size, compressed_size);
        assert_eq!(data[..length], decompressed[..length]);
    }

    #[test]
    fn test_write_empty_array_throws() -> Result<(), Box<dyn std::error::Error>> {
        let data: Vec<f32> = vec![];
        let compressed = OmFileWriter::new(0, 0, 0, 0).write_all_in_memory(
            CompressionType::P4nzdec256,
            1.0,
            &data,
        );
        // make sure there was an error and it is of the correct type
        assert!(compressed.is_err());
        let err = compressed.err().unwrap();
        // make sure the error is of the correct type
        assert_eq!(err, OmFilesRsError::DimensionMustBeLargerThan0);

        Ok(())
    }

    #[test]
    fn test_in_memory_int_compression() -> Result<(), Box<dyn std::error::Error>> {
        let data: Vec<f32> = vec![
            0.0, 5.0, 2.0, 3.0, 2.0, 5.0, 6.0, 2.0, 8.0, 3.0, 10.0, 14.0, 12.0, 15.0, 14.0, 15.0,
            66.0, 17.0, 12.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
        ];
        let must_equal = data.clone();
        let compressed = OmFileWriter::new(1, data.len(), 1, 10).write_all_in_memory(
            CompressionType::P4nzdec256,
            1.0,
            &data,
        )?;

        assert_eq!(compressed.count(), 212);

        let uncompressed = OmFileReader::new(compressed)
            .expect("Could not get data from backend")
            .read_all()?;

        assert_eq_with_accuracy(&must_equal, &uncompressed, 0.001);

        Ok(())
    }

    #[test]
    fn test_in_memory_f32_compression() -> Result<(), Box<dyn std::error::Error>> {
        let data: Vec<f32> = vec![
            0.0, 5.0, 2.0, 3.0, 2.0, 5.0, 6.0, 2.0, 8.0, 3.0, 10.0, 14.0, 12.0, 15.0, 14.0, 15.0,
            66.0, 17.0, 12.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
        ];
        let must_equal = data.clone();
        let compressed = OmFileWriter::new(1, data.len(), 1, 10).write_all_in_memory(
            CompressionType::Fpxdec32,
            1.0,
            &data,
        )?;

        assert_eq!(compressed.count(), 236);

        let uncompressed = OmFileReader::new(compressed)
            .expect("Could not get data from backend")
            .read_all()?;

        assert_eq_with_accuracy(&must_equal, &uncompressed, 0.001);

        Ok(())
    }

    #[test]
    fn test_write_more_data_than_expected() -> Result<(), Box<dyn std::error::Error>> {
        let file = "writetest_failing.om";
        remove_file_if_exists(file);

        let result0 = Arc::new((0..10).map(|x| x as f32).collect::<Vec<f32>>());
        let result2 = Arc::new((10..20).map(|x| x as f32).collect::<Vec<f32>>());
        let result4 = Arc::new((20..30).map(|x| x as f32).collect::<Vec<f32>>());

        // Attempt to write more data than expected and ensure it throws an error
        let result = OmFileWriter::new(5, 5, 2, 2).write_to_file(
            file,
            CompressionType::P4nzdec256,
            1.0,
            false,
            |dim0pos| match dim0pos {
                0 => Ok(result0.as_slice()),
                2 => Ok(result2.as_slice()),
                4 => Ok(result4.as_slice()),
                _ => panic!("Not expected"),
            },
        );

        // Ensure that an error was thrown
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err, OmFilesRsError::ChunkHasWrongNumberOfElements);

        // Remove the temporary file if it exists
        let temp_file = format!("{}~", file);
        remove_file_if_exists(&temp_file);

        Ok(())
    }

    #[test]
    fn test_write_large() -> Result<(), Box<dyn std::error::Error>> {
        let file = "writetest.om";
        std::fs::remove_file(file).ok();

        let mut writer = OmFileEncoder::new(
            vec![100, 100, 10],
            vec![2, 2, 2],
            CompressionType::P4nzdec256,
            1.0,
            256,
        );
        let mut buffer = OmFileBufferedWriter::new(writer.output_buffer_capacity());

        let mut file_handle = File::create(file)?;
        let mut file_handle_borrowed = file_handle.borrow_mut();

        let data: Vec<f32> = (0..100000).map(|x| (x % 10000) as f32).collect();
        buffer.write_header(&mut file_handle_borrowed)?;
        writer.write_data(
            &data,
            &[100, 100, 10],
            &[0..100, 0..100, 0..10],
            &mut file_handle_borrowed,
            &mut buffer,
        )?;
        let lut_start = buffer.total_bytes_written;
        let lut_chunk_length = writer.write_lut(&mut buffer, &mut file_handle_borrowed)?;
        let json_variable = OmFileJSONVariable {
            name: None,
            dimensions: writer.dims.clone(),
            chunks: writer.chunks.clone(),
            dimension_names: None,
            scalefactor: writer.scalefactor,
            compression: writer.compression.to_c(),
            data_type: om_datatype_t_DATA_TYPE_FLOAT,
            lut_offset: lut_start,
            lut_chunk_size: lut_chunk_length,
        };
        let json = OmFileJSON {
            variables: vec![json_variable],
            some_attributes: None,
        };
        buffer.write_trailer(&json, &mut file_handle_borrowed)?;

        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;

        let read = OmFileReader2::open_file(read_backend, 256)?;

        let a1 = read.read_simple(&[50..51, 20..21, 1..2], 65536, 512)?;
        assert_eq!(a1, vec![201.0]);

        let a = read.read_simple(&[0..100, 0..100, 0..10], 65536, 512)?;
        assert_eq!(a.len(), data.len());
        let range = 0..100; // a.len() - 100..a.len() - 1
        assert_eq!(a[range.clone()], data[range]);

        Ok(())
    }

    #[test]
    fn test_write_chunks() -> Result<(), Box<dyn std::error::Error>> {
        let file = "writetest.om";
        remove_file_if_exists(file);

        let mut writer = OmFileEncoder::new(
            vec![5, 5],
            vec![2, 2],
            CompressionType::P4nzdec256,
            1.0,
            256,
        );

        let mut buffer = OmFileBufferedWriter::new(writer.output_buffer_capacity());

        let mut file_handle = File::create(file)?;
        let mut file_handle = file_handle.borrow_mut();

        // Directly feed individual chunks
        buffer.write_header(&mut file_handle)?;
        writer.write_data(
            &[0.0, 1.0, 5.0, 6.0],
            &[2, 2],
            &[0..2, 0..2],
            &mut file_handle,
            &mut buffer,
        )?;
        writer.write_data(
            &[2.0, 3.0, 7.0, 8.0],
            &[2, 2],
            &[0..2, 0..2],
            &mut file_handle,
            &mut buffer,
        )?;
        writer.write_data(
            &[4.0, 9.0],
            &[2, 1],
            &[0..2, 0..1],
            &mut file_handle,
            &mut buffer,
        )?;
        writer.write_data(
            &[10.0, 11.0, 15.0, 16.0],
            &[2, 2],
            &[0..2, 0..2],
            &mut file_handle,
            &mut buffer,
        )?;
        writer.write_data(
            &[12.0, 13.0, 17.0, 18.0],
            &[2, 2],
            &[0..2, 0..2],
            &mut file_handle,
            &mut buffer,
        )?;
        writer.write_data(
            &[14.0, 19.0],
            &[2, 1],
            &[0..2, 0..1],
            &mut file_handle,
            &mut buffer,
        )?;
        writer.write_data(
            &[20.0, 21.0],
            &[1, 2],
            &[0..1, 0..2],
            &mut file_handle,
            &mut buffer,
        )?;
        writer.write_data(
            &[22.0, 23.0],
            &[1, 2],
            &[0..1, 0..2],
            &mut file_handle,
            &mut buffer,
        )?;
        writer.write_data(
            &[24.0],
            &[1, 1],
            &[0..1, 0..1],
            &mut file_handle,
            &mut buffer,
        )?;

        let lut_start = buffer.total_bytes_written;
        let lut_chunk_length = writer.write_lut(&mut buffer, &mut file_handle)?;
        let json_variable = OmFileJSONVariable {
            name: None,
            dimensions: writer.dims.clone(),
            chunks: writer.chunks.clone(),
            dimension_names: None,
            scalefactor: writer.scalefactor,
            compression: writer.compression.to_c(),
            data_type: om_datatype_t_DATA_TYPE_FLOAT,
            lut_offset: lut_start,
            lut_chunk_size: lut_chunk_length,
        };
        let json = OmFileJSON {
            variables: vec![json_variable],
            some_attributes: None,
        };
        buffer.write_trailer(&json, &mut file_handle)?;

        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        let read = OmFileReader2::open_file(read_backend, 256)?;

        let a = read.read_simple(&[0..5, 0..5], 65536, 512)?;
        let expected = vec![
            0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0,
            16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
        ];
        assert_eq!(a, expected);

        Ok(())
    }

    #[test]
    fn test_offset_write() -> Result<(), Box<dyn std::error::Error>> {
        let file = "writetest.om";
        remove_file_if_exists(file);

        let mut writer = OmFileEncoder::new(
            vec![5, 5],
            vec![2, 2],
            CompressionType::P4nzdec256,
            1.0,
            256,
        );

        let mut buffer = OmFileBufferedWriter::new(writer.output_buffer_capacity());

        let mut file_handle = File::create(file)?;
        let mut file_handle = &mut file_handle;

        // Deliberately add NaN on all positions that should not be written to the file.
        // Only the inner 5x5 array is written.
        let data = vec![
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
            0.0,
            1.0,
            2.0,
            3.0,
            4.0,
            std::f32::NAN,
            std::f32::NAN,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            std::f32::NAN,
            std::f32::NAN,
            10.0,
            11.0,
            12.0,
            13.0,
            14.0,
            std::f32::NAN,
            std::f32::NAN,
            15.0,
            16.0,
            17.0,
            18.0,
            19.0,
            std::f32::NAN,
            std::f32::NAN,
            20.0,
            21.0,
            22.0,
            23.0,
            24.0,
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
            std::f32::NAN,
        ];

        buffer.write_header(&mut file_handle)?;
        writer.write_data(&data, &[7, 7], &[1..6, 1..6], &mut file_handle, &mut buffer)?;

        let lut_start = buffer.total_bytes_written;
        let lut_chunk_length = writer.write_lut(&mut buffer, &mut file_handle)?;
        let json_variable = OmFileJSONVariable {
            name: None,
            dimensions: writer.dims.clone(),
            chunks: writer.chunks.clone(),
            dimension_names: None,
            scalefactor: writer.scalefactor,
            compression: writer.compression.to_c(),
            data_type: om_datatype_t_DATA_TYPE_FLOAT,
            lut_offset: lut_start,
            lut_chunk_size: lut_chunk_length,
        };
        let json = OmFileJSON {
            variables: vec![json_variable],
            some_attributes: None,
        };
        buffer.write_trailer(&json, &mut file_handle)?;

        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        let read = OmFileReader2::open_file(read_backend, 256)?;

        let a = read.read_simple(&[0..5, 0..5], 65536, 512)?;
        let expected = vec![
            0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0,
            16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
        ];
        assert_eq!(a, expected);

        Ok(())
    }

    #[test]
    fn test_write_3d() -> Result<(), Box<dyn std::error::Error>> {
        let file = "writetest.om";
        remove_file_if_exists(file);

        let dims = vec![3, 3, 3];
        let mut writer = OmFileEncoder::new(
            dims.clone(),
            vec![2, 2, 2],
            CompressionType::P4nzdec256,
            1.0,
            256,
        );

        let mut buffer = OmFileBufferedWriter::new(writer.output_buffer_capacity());
        let mut file_handle = File::create(file)?;
        let mut file_handle = &mut file_handle;

        let data: Vec<f32> = (0..27).map(|x| x as f32).collect();

        buffer.write_header(&mut file_handle)?;
        writer.write_data(
            &data,
            &dims,
            &[0..3, 0..3, 0..3],
            &mut file_handle,
            &mut buffer,
        )?;

        let lut_start = buffer.total_bytes_written;
        let lut_chunk_length = writer.write_lut(&mut buffer, &mut file_handle)?;
        let json_variable = OmFileJSONVariable {
            name: None,
            dimensions: writer.dims.clone(),
            chunks: writer.chunks.clone(),
            dimension_names: None,
            scalefactor: writer.scalefactor,
            compression: writer.compression.to_c(),
            data_type: om_datatype_t_DATA_TYPE_FLOAT,
            lut_offset: lut_start,
            lut_chunk_size: lut_chunk_length,
        };
        let json = OmFileJSON {
            variables: vec![json_variable],
            some_attributes: None,
        };
        buffer.write_trailer(&json, &mut file_handle)?;

        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        let read = OmFileReader2::open_file(read_backend, 256)?;

        let a = read.read_simple(&[0..3, 0..3, 0..3], 65536, 512)?;
        assert_eq!(a, data);

        let dims_u64: Vec<u64> = dims.iter().map(|&x| x as u64).collect();
        for x in 0..dims_u64[0] {
            for y in 0..dims_u64[1] {
                for z in 0..dims_u64[2] {
                    let value = read.read_simple(&[x..x + 1, y..y + 1, z..z + 1], 65536, 512)?;
                    assert_eq!(value[0], (x * 9 + y * 3 + z) as f32);
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_write_v3() -> Result<(), Box<dyn std::error::Error>> {
        let file = "writetest.om";
        remove_file_if_exists(file);

        let dims = vec![5, 5];
        let mut writer = OmFileEncoder::new(
            dims.clone(),
            vec![2, 2],
            CompressionType::P4nzdec256,
            1.0,
            2, // lut_chunk_element_count
        );

        let mut buffer = OmFileBufferedWriter::new(writer.output_buffer_capacity());
        let mut file_handle = File::create(file)?;
        let mut file_handle = &mut file_handle;

        let data: Vec<f32> = (0..25).map(|x| x as f32).collect();
        buffer.write_header(&mut file_handle)?;
        writer.write_data(&data, &dims, &[0..5, 0..5], &mut file_handle, &mut buffer)?;

        let lut_start = buffer.total_bytes_written;
        let lut_chunk_length = writer.write_lut(&mut buffer, &mut file_handle)?;
        let json_variable = OmFileJSONVariable {
            name: None,
            dimensions: writer.dims.clone(),
            chunks: writer.chunks.clone(),
            dimension_names: None,
            scalefactor: writer.scalefactor,
            compression: writer.compression.to_c(),
            data_type: om_datatype_t_DATA_TYPE_FLOAT,
            lut_offset: lut_start,
            lut_chunk_size: lut_chunk_length,
        };
        let json = OmFileJSON {
            variables: vec![json_variable],
            some_attributes: None,
        };
        buffer.write_trailer(&json, &mut file_handle)?;

        let file_for_reading = File::open(file)?;
        let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
        let read = OmFileReader2::open_file(read_backend, 256)?;

        let a = read.read_simple(&[0..5, 0..5], 65536, 512)?;
        assert_eq!(a, data);

        // Single index
        for x in 0..dims[0] {
            for y in 0..dims[1] {
                let value = read.read_simple(&[x..x + 1, y..y + 1], 65536, 512)?;
                assert_eq!(value, vec![(x * 5 + y) as f32]);
            }
        }

        // Read into an existing array with an offset
        for x in 0..dims[0] {
            for y in 0..dims[1] {
                let mut r = vec![std::f32::NAN; 9];
                read.read(&mut r, &[x..x + 1, y..y + 1], &[1, 1], &[3, 3], 65536, 512)?;
                let expected = vec![
                    std::f32::NAN,
                    std::f32::NAN,
                    std::f32::NAN,
                    std::f32::NAN,
                    (x * 5 + y) as f32,
                    std::f32::NAN,
                    std::f32::NAN,
                    std::f32::NAN,
                    std::f32::NAN,
                ];
                assert_eq!(r, expected);
            }
        }

        // 2x in fast dim
        for x in 0..dims[0] {
            for y in 0..dims[1] - 1 {
                let value = read.read_simple(&[x..x + 1, y..y + 2], 65536, 512)?;
                assert_eq!(value, vec![(x * 5 + y) as f32, (x * 5 + y + 1) as f32]);
            }
        }

        // 2x in slow dim
        for x in 0..dims[0] - 1 {
            for y in 0..dims[1] {
                let value = read.read_simple(&[x..x + 2, y..y + 1], 65536, 512)?;
                assert_eq!(value, vec![(x * 5 + y) as f32, ((x + 1) * 5 + y) as f32]);
            }
        }

        // 2x2
        for x in 0..dims[0] - 1 {
            for y in 0..dims[1] - 1 {
                let value = read.read_simple(&[x..x + 2, y..y + 2], 65536, 512)?;
                let expected = vec![
                    (x * 5 + y) as f32,
                    (x * 5 + y + 1) as f32,
                    ((x + 1) * 5 + y) as f32,
                    ((x + 1) * 5 + y + 1) as f32,
                ];
                assert_eq!(value, expected);
            }
        }

        // 3x3
        for x in 0..dims[0] - 2 {
            for y in 0..dims[1] - 2 {
                let value = read.read_simple(&[x..x + 3, y..y + 3], 65536, 512)?;
                let expected = vec![
                    (x * 5 + y) as f32,
                    (x * 5 + y + 1) as f32,
                    (x * 5 + y + 2) as f32,
                    ((x + 1) * 5 + y) as f32,
                    ((x + 1) * 5 + y + 1) as f32,
                    ((x + 1) * 5 + y + 2) as f32,
                    ((x + 2) * 5 + y) as f32,
                    ((x + 2) * 5 + y + 1) as f32,
                    ((x + 2) * 5 + y + 2) as f32,
                ];
                assert_eq!(value, expected);
            }
        }

        // 1x5
        for x in 0..dims[0] {
            let value = read.read_simple(&[x..x + 1, 0..5], 65536, 512)?;
            let expected: Vec<f32> = (0..5).map(|y| (x * 5 + y) as f32).collect();
            assert_eq!(value, expected);
        }

        // 5x1
        for y in 0..dims[1] {
            let value = read.read_simple(&[0..5, y..y + 1], 65536, 512)?;
            let expected: Vec<f32> = (0..5).map(|x| (x * 5 + y) as f32).collect();
            assert_eq!(value, expected);
        }

        std::fs::remove_file(file)?;
        Ok(())
    }

    #[test]
    fn test_nan() -> Result<(), Box<dyn std::error::Error>> {
        let file = "writetest_nan.om";
        remove_file_if_exists(file);

        let data: Vec<f32> = (0..(5 * 5)).map(|_| f32::NAN).collect();

        OmFileWriter::new(5, 5, 5, 5).write_to_file(
            file,
            CompressionType::P4nzdec256,
            1.0,
            false,
            |_| Ok(data.as_slice()),
        )?;

        let reader = OmFileReader::from_file(file)?;

        // assert that all values are nan
        assert!(reader
            .read_range(Some(1..2), Some(1..2))?
            .iter()
            .all(|x| x.is_nan()));

        remove_file_if_exists(file);

        Ok(())
    }

    #[test]
    fn test_write() -> Result<(), OmFilesRsError> {
        let file = "writetest.om";
        remove_file_if_exists(file);

        let result0 = Arc::new((0..10).map(|x| x as f32).collect::<Vec<f32>>());
        let result2 = Arc::new((10..20).map(|x| x as f32).collect::<Vec<f32>>());
        let result4 = Arc::new((20..25).map(|x| x as f32).collect::<Vec<f32>>());

        OmFileWriter::new(5, 5, 2, 2).write_to_file(
            file,
            CompressionType::P4nzdec256,
            1.0,
            false,
            |dim0pos| match dim0pos {
                0 => Ok(result0.as_slice()),
                2 => Ok(result2.as_slice()),
                4 => Ok(result4.as_slice()),
                _ => panic!("Not expected"),
            },
        )?;

        let read = OmFileReader::from_file(file)?;
        let a = read.read_range(Some(0..5), Some(0..5))?;
        assert_eq!(
            a,
            vec![
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0,
                15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0
            ]
        );

        // single index
        for x in 0..read.dimensions.dim0 {
            for y in 0..read.dimensions.dim1 {
                assert_eq!(
                    read.read_range(Some(x..x + 1), Some(y..y + 1))?,
                    vec![x as f32 * 5.0 + y as f32]
                );
            }
        }

        // 2x in fast dim
        for x in 0..read.dimensions.dim0 {
            for y in 0..read.dimensions.dim1 - 1 {
                assert_eq!(
                    read.read_range(Some(x..x + 1), Some(y..y + 2))?,
                    vec![x as f32 * 5.0 + y as f32, x as f32 * 5.0 + y as f32 + 1.0]
                );
            }
        }

        // 2x in slow dim
        for x in 0..read.dimensions.dim0 - 1 {
            for y in 0..read.dimensions.dim1 {
                assert_eq!(
                    read.read_range(Some(x..x + 2), Some(y..y + 1))?,
                    vec![x as f32 * 5.0 + y as f32, (x as f32 + 1.0) * 5.0 + y as f32]
                );
            }
        }

        // 2x2
        for x in 0..read.dimensions.dim0 - 1 {
            for y in 0..read.dimensions.dim1 - 1 {
                assert_eq!(
                    read.read_range(Some(x..x + 2), Some(y..y + 2))?,
                    vec![
                        x as f32 * 5.0 + y as f32,
                        x as f32 * 5.0 + y as f32 + 1.0,
                        (x as f32 + 1.0) * 5.0 + y as f32,
                        (x as f32 + 1.0) * 5.0 + y as f32 + 1.0
                    ]
                );
            }
        }

        // 3x3
        for x in 0..read.dimensions.dim0 - 2 {
            for y in 0..read.dimensions.dim1 - 2 {
                assert_eq!(
                    read.read_range(Some(x..x + 3), Some(y..y + 3))?,
                    vec![
                        x as f32 * 5.0 + y as f32,
                        x as f32 * 5.0 + y as f32 + 1.0,
                        x as f32 * 5.0 + y as f32 + 2.0,
                        (x as f32 + 1.0) * 5.0 + y as f32,
                        (x as f32 + 1.0) * 5.0 + y as f32 + 1.0,
                        (x as f32 + 1.0) * 5.0 + y as f32 + 2.0,
                        (x as f32 + 2.0) * 5.0 + y as f32,
                        (x as f32 + 2.0) * 5.0 + y as f32 + 1.0,
                        (x as f32 + 2.0) * 5.0 + y as f32 + 2.0
                    ]
                );
            }
        }

        // 1x5
        for x in 0..read.dimensions.dim1 {
            assert_eq!(
                read.read_range(Some(x..x + 1), Some(0..5))?,
                vec![
                    x as f32 * 5.0,
                    x as f32 * 5.0 + 1.0,
                    x as f32 * 5.0 + 2.0,
                    x as f32 * 5.0 + 3.0,
                    x as f32 * 5.0 + 4.0
                ]
            );
        }

        // 5x1
        for x in 0..read.dimensions.dim0 {
            assert_eq!(
                read.read_range(Some(0..5), Some(x..x + 1))?,
                vec![
                    x as f32,
                    x as f32 + 5.0,
                    x as f32 + 10.0,
                    x as f32 + 15.0,
                    x as f32 + 20.0
                ]
            );
        }

        // // test interpolation
        // assert_eq!(
        //     read.read_interpolated(0, 0.5, 0, 0.5, 2, 0..5)?,
        //     vec![7.5, 8.5, 9.5, 10.5, 11.5]
        // );
        // assert_eq!(
        //     read.read_interpolated(0, 0.1, 0, 0.2, 2, 0..5)?,
        //     vec![2.5, 3.4999998, 4.5, 5.5, 6.5]
        // );
        // assert_eq!(
        //     read.read_interpolated(0, 0.9, 0, 0.2, 2, 0..5)?,
        //     vec![6.5, 7.5, 8.5, 9.5, 10.5]
        // );
        // assert_eq!(
        //     read.read_interpolated(0, 0.1, 0, 0.9, 2, 0..5)?,
        //     vec![9.5, 10.499999, 11.499999, 12.5, 13.499999]
        // );
        // assert_eq!(
        //     read.read_interpolated(0, 0.8, 0, 0.9, 2, 0..5)?,
        //     vec![12.999999, 14.0, 15.0, 16.0, 17.0]
        // );

        Ok(())
    }

    #[test]
    fn test_write_fpx() -> Result<(), Box<dyn std::error::Error>> {
        let file = "writetest_fpx.om";
        remove_file_if_exists(file);

        let result0 = Arc::new((0..10).map(|x| x as f32).collect::<Vec<f32>>());
        let result2 = Arc::new((10..20).map(|x| x as f32).collect::<Vec<f32>>());
        let result4 = Arc::new((20..25).map(|x| x as f32).collect::<Vec<f32>>());

        OmFileWriter::new(5, 5, 2, 2).write_to_file(
            file,
            CompressionType::Fpxdec32,
            1.0,
            false,
            |dim0pos| match dim0pos {
                0 => Ok(result0.as_slice()),
                2 => Ok(result2.as_slice()),
                4 => Ok(result4.as_slice()),
                _ => panic!("Not expected"),
            },
        )?;

        let reader = OmFileReader::from_file(file)?;
        let a = reader.read_range(Some(0..5), Some(0..5))?;
        assert_eq!(
            a,
            vec![
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0,
                15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0
            ]
        );

        // single index
        for x in 0..reader.dimensions.dim0 {
            for y in 0..reader.dimensions.dim1 {
                assert_eq!(
                    reader.read_range(Some(x..x + 1), Some(y..y + 1))?,
                    vec![x as f32 * 5.0 + y as f32]
                );
            }
        }

        // 2x in fast dim
        for x in 0..reader.dimensions.dim0 {
            for y in 0..reader.dimensions.dim1 - 1 {
                assert_eq!(
                    reader.read_range(Some(x..x + 1), Some(y..y + 2))?,
                    vec![x as f32 * 5.0 + y as f32, x as f32 * 5.0 + y as f32 + 1.0]
                );
            }
        }

        // 2x in slow dim
        for x in 0..reader.dimensions.dim0 - 1 {
            for y in 0..reader.dimensions.dim1 {
                assert_eq!(
                    reader.read_range(Some(x..x + 2), Some(y..y + 1))?,
                    vec![x as f32 * 5.0 + y as f32, (x as f32 + 1.0) * 5.0 + y as f32]
                );
            }
        }

        // 2x2
        for x in 0..reader.dimensions.dim0 - 1 {
            for y in 0..reader.dimensions.dim1 - 1 {
                assert_eq!(
                    reader.read_range(Some(x..x + 2), Some(y..y + 2))?,
                    vec![
                        x as f32 * 5.0 + y as f32,
                        x as f32 * 5.0 + y as f32 + 1.0,
                        (x as f32 + 1.0) * 5.0 + y as f32,
                        (x as f32 + 1.0) * 5.0 + y as f32 + 1.0
                    ]
                );
            }
        }

        // 3x3
        for x in 0..reader.dimensions.dim0 - 2 {
            for y in 0..reader.dimensions.dim1 - 2 {
                assert_eq!(
                    reader.read_range(Some(x..x + 3), Some(y..y + 3))?,
                    vec![
                        x as f32 * 5.0 + y as f32,
                        x as f32 * 5.0 + y as f32 + 1.0,
                        x as f32 * 5.0 + y as f32 + 2.0,
                        (x as f32 + 1.0) * 5.0 + y as f32,
                        (x as f32 + 1.0) * 5.0 + y as f32 + 1.0,
                        (x as f32 + 1.0) * 5.0 + y as f32 + 2.0,
                        (x as f32 + 2.0) * 5.0 + y as f32,
                        (x as f32 + 2.0) * 5.0 + y as f32 + 1.0,
                        (x as f32 + 2.0) * 5.0 + y as f32 + 2.0
                    ]
                );
            }
        }

        // 1x5
        for x in 0..reader.dimensions.dim1 {
            assert_eq!(
                reader.read_range(Some(x..x + 1), Some(0..5))?,
                vec![
                    x as f32 * 5.0,
                    x as f32 * 5.0 + 1.0,
                    x as f32 * 5.0 + 2.0,
                    x as f32 * 5.0 + 3.0,
                    x as f32 * 5.0 + 4.0
                ]
            );
        }

        // 5x1
        for x in 0..reader.dimensions.dim0 {
            assert_eq!(
                reader.read_range(Some(0..5), Some(x..x + 1))?,
                vec![
                    x as f32,
                    x as f32 + 5.0,
                    x as f32 + 10.0,
                    x as f32 + 15.0,
                    x as f32 + 20.0
                ]
            );
        }

        remove_file_if_exists(file);

        Ok(())
    }

    fn assert_eq_with_accuracy(expected: &[f32], actual: &[f32], accuracy: f32) {
        assert_eq!(expected.len(), actual.len());
        for (e, a) in expected.iter().zip(actual.iter()) {
            assert!((e - a).abs() < accuracy, "Expected: {}, Actual: {}", e, a);
        }
    }

    fn remove_file_if_exists(file: &str) {
        if fs::metadata(file).is_ok() {
            fs::remove_file(file).unwrap();
        }
    }
}
