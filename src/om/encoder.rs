use omfileformatc_rs::delta2d_encode;
use omfileformatc_rs::{delta2d_encode_xor, fpxenc32, p4ndenc64, p4nzenc128v16};
use std::cmp::min;
use std::ops::Range;
use std::slice;

use crate::compression::p4nenc256_bound;
use crate::compression::CompressionType;
// use crate::delta2d::delta2d_encode;
use crate::om::header::OmHeader;
use crate::utils::{divide_rounded_up, divide_rounded_up_u64};

use super::backends::OmFileWriterBackend;
use super::errors::OmFilesRsError;
use super::omfile_json::OmFileJSON;

pub struct OmFileBufferedWriter {
    pub buffer: Vec<u8>,
    pub write_position: u64,
    pub total_bytes_written: u64,
    pub capacity: u64,
}

impl OmFileBufferedWriter {
    pub fn new(capacity: u64) -> Self {
        Self {
            buffer: vec![0; capacity as usize],
            write_position: 0,
            total_bytes_written: 0,
            capacity,
        }
    }

    pub fn write_header<Backend: OmFileWriterBackend>(
        &mut self,
        backend: &mut Backend,
    ) -> Result<(), OmFilesRsError> {
        self.write_header_internal();
        backend.write(&self.buffer[0..self.write_position as usize])?;
        self.write_position = 0;
        Ok(())
    }

    pub fn write_trailer<Backend: OmFileWriterBackend>(
        &mut self,
        meta: &OmFileJSON,
        backend: &mut Backend,
    ) -> Result<(), OmFilesRsError> {
        self.write_trailer_internal(meta)?;
        backend.write(&self.buffer[0..self.write_position as usize])?;
        self.write_position = 0;
        Ok(())
    }

    fn write_header_internal(&mut self) {
        assert!(self.capacity - self.write_position >= 3);
        self.buffer[self.write_position as usize] = OmHeader::MAGIC_NUMBER1;
        self.buffer[self.write_position as usize + 1] = OmHeader::MAGIC_NUMBER2;
        self.buffer[self.write_position as usize + 2] = 3;
        self.write_position += 3;
        self.total_bytes_written += 3;
    }

    fn write_trailer_internal(&mut self, meta: &OmFileJSON) -> Result<(), OmFilesRsError> {
        let json = serde_json::to_vec(meta).map_err(|_| OmFilesRsError::JSONSerializationError)?;
        assert!(self.capacity - self.write_position >= json.len() as u64);
        let json_length = json.len() as u64;
        self.buffer[self.write_position as usize..(self.write_position + json_length) as usize]
            .copy_from_slice(&json);
        self.write_position += json_length;
        self.total_bytes_written += json_length;

        assert!(self.capacity - self.write_position >= 8);
        let json_length_bytes = json_length.to_le_bytes();
        self.buffer[self.write_position as usize..(self.write_position + 8) as usize]
            .copy_from_slice(&json_length_bytes);
        self.write_position += 8;
        self.total_bytes_written += 8;

        Ok(())
    }
}

impl Drop for OmFileBufferedWriter {
    fn drop(&mut self) {
        // No need to manually deallocate the buffer as Vec handles it
    }
}

pub struct OmFileEncoder {
    pub scalefactor: f32,
    pub compression: CompressionType,
    pub dims: Vec<u64>,
    pub chunks: Vec<u64>,
    pub chunk_offset_bytes: Vec<u64>,
    pub chunk_buffer: Vec<u8>,
    pub chunk_index: usize,
    pub lut_chunk_element_count: usize,
}

impl OmFileEncoder {
    pub fn new(
        dimensions: Vec<u64>,
        chunk_dimensions: Vec<u64>,
        compression: CompressionType,
        scalefactor: f32,
        lut_chunk_element_count: usize,
    ) -> Self {
        let n_chunks: u64 = dimensions
            .iter()
            .zip(&chunk_dimensions)
            .map(|(d, c)| divide_rounded_up_u64(d, c))
            .product();
        let chunk_size_byte = chunk_dimensions.iter().product::<u64>() * 4;
        if chunk_size_byte > 1024 * 1024 * 4 {
            println!(
                "WARNING: Chunk size greater than 4 MB ({} MB)!",
                chunk_size_byte as f32 / 1024.0 / 1024.0
            );
        }
        let buffer_size = p4nenc256_bound(chunk_dimensions.iter().product::<u64>() as usize, 4);
        Self {
            scalefactor,
            compression,
            dims: dimensions,
            chunks: chunk_dimensions,
            chunk_offset_bytes: vec![0; n_chunks as usize + 1],
            chunk_buffer: vec![0; buffer_size],
            chunk_index: 0,
            lut_chunk_element_count,
        }
    }

    /// Return the total number of chunks in this file
    pub fn number_of_chunks(&self) -> u64 {
        self.dims
            .iter()
            .zip(&self.chunks)
            .map(|(dim, chunk)| divide_rounded_up_u64(dim, chunk))
            .product()
    }

    /// Calculate the size of the output buffer.
    pub fn output_buffer_capacity(&self) -> u64 {
        let buffer_size = p4nenc256_bound(
            self.chunks.iter().product::<u64>() as usize,
            self.compression.bytes_per_element(),
        ) as u64;

        let n_chunks = self
            .dims
            .iter()
            .zip(&self.chunks)
            .map(|(dim, chunk)| divide_rounded_up_u64(dim, chunk))
            .product::<u64>();

        // Assume the LUT buffer is not compressible
        let lut_buffer_size = n_chunks * 8;

        std::cmp::max(4096, std::cmp::max(lut_buffer_size, buffer_size))
    }

    pub fn write_data<Backend: OmFileWriterBackend>(
        &mut self,
        array: &[f32],
        array_dimensions: &[u64],
        array_read: &[Range<u64>],
        backend: &mut Backend,
        out: &mut OmFileBufferedWriter,
    ) -> Result<(), OmFilesRsError> {
        let number_of_chunks_in_array = array_read
            .iter()
            .zip(&self.chunks)
            .map(|(r, c)| (r.end - r.start + c - 1) / c)
            .product();
        let mut c_offset = Some(0);
        while let Some(offset) = c_offset {
            c_offset = self.write_next_chunks(
                array,
                array_dimensions,
                &array_read.iter().map(|r| r.start).collect::<Vec<_>>(),
                &array_read
                    .iter()
                    .map(|r| r.end - r.start)
                    .collect::<Vec<_>>(),
                offset,
                number_of_chunks_in_array,
                out,
            );
            backend.write(&out.buffer[0..out.write_position as usize])?;
            out.write_position = 0;
        }
        Ok(())
    }

    pub fn write_lut<Backend: OmFileWriterBackend>(
        &mut self,
        out: &mut OmFileBufferedWriter,
        backend: &mut Backend,
    ) -> Result<u64, OmFilesRsError> {
        let lut_chunk_length = self.write_lut_internal(out);
        backend.write(&out.buffer[0..out.write_position as usize])?;
        out.write_position = 0;
        Ok(lut_chunk_length)
    }

    fn write_lut_internal(&mut self, out: &mut OmFileBufferedWriter) -> u64 {
        let mut max_length = 0;

        println!("lut_chunk_element_count: {}", self.lut_chunk_element_count);
        println!(
            "chunk_offset_bytes.len(): {}",
            self.chunk_offset_bytes.len()
        );
        // println!(
        //     "chunk_offset_bytes: {:?}",
        //     self.chunk_offset_bytes.as_slice()
        // );

        // Calculate maximum chunk size
        for i in 0..divide_rounded_up(self.chunk_offset_bytes.len(), self.lut_chunk_element_count) {
            let range_start = i * self.lut_chunk_element_count;
            let range_end = std::cmp::min(
                (i + 1) * self.lut_chunk_element_count,
                self.chunk_offset_bytes.len(),
            );
            let len = unsafe {
                p4ndenc64(
                    self.chunk_offset_bytes.as_ptr().add(range_start) as *mut u64,
                    range_end - range_start,
                    out.buffer.as_mut_ptr().add(out.write_position as usize),
                )
            };
            if len > max_length {
                max_length = len;
            }
        }
        println!("max_length: {}", max_length);

        // Write chunks to buffer and pad all chunks to have `max_length` bytes
        for chunk in self.chunk_offset_bytes.chunks(self.lut_chunk_element_count) {
            let len = unsafe {
                p4ndenc64(
                    chunk.as_ptr() as *mut u64,
                    chunk.len(),
                    out.buffer[out.write_position as usize..].as_mut_ptr(),
                )
            };
            out.write_position += max_length as u64;
            out.total_bytes_written += max_length as u64;
        }

        max_length as u64
    }

    fn write_next_chunks(
        &mut self,
        array: &[f32],
        array_dimensions: &[u64],
        array_offset: &[u64],
        array_count: &[u64],
        mut c_offset: u64,
        number_of_chunks_in_array: u64,
        out: &mut OmFileBufferedWriter,
    ) -> Option<u64> {
        assert_eq!(
            array.len(),
            array_dimensions.iter().product::<u64>() as usize
        );

        while c_offset < number_of_chunks_in_array {
            let mut rolling_multiply = 1;
            let mut rolling_multiply_chunk_length = 1;
            let mut rolling_multiply_target_cube = 1;

            let mut read_coordinate = 0usize;
            let mut write_coordinate = 0usize;
            let mut linear_read_count = 1u64;
            let mut linear_read = true;
            let mut length_last = 0u64;

            // Calculate number of elements in this chunk and initial coordinates
            for i in (0..self.dims.len()).rev() {
                let n_chunks_in_this_dimension =
                    (self.dims[i] + self.chunks[i] - 1) / self.chunks[i];
                let c0 = (self.chunk_index as u64 / rolling_multiply) % n_chunks_in_this_dimension;
                let c0_offset = (c_offset / rolling_multiply) % n_chunks_in_this_dimension;
                let length0 = min((c0 + 1) * self.chunks[i], self.dims[i]) - c0 * self.chunks[i];

                if i == self.dims.len() - 1 {
                    length_last = length0;
                }

                read_coordinate += (c0_offset * self.chunks[i] + array_offset[i]) as usize
                    * rolling_multiply_target_cube as usize;

                assert!(length0 <= array_count[i]);
                assert!(length0 <= array_dimensions[i]);

                if i == self.dims.len() - 1
                    && !(array_count[i] == length0 && array_dimensions[i] == length0)
                {
                    linear_read_count = length0;
                    linear_read = false;
                }

                if linear_read && array_count[i] == length0 && array_dimensions[i] == length0 {
                    linear_read_count *= length0;
                } else {
                    linear_read = false;
                }

                rolling_multiply *= n_chunks_in_this_dimension;
                rolling_multiply_target_cube *= array_dimensions[i];
                rolling_multiply_chunk_length *= length0;
            }

            let length_in_chunk = rolling_multiply_chunk_length;

            // Loop over elements to read and move to target buffer
            'loop_buffer: loop {
                match self.compression {
                    CompressionType::P4nzdec256 => {
                        let chunk_buffer = unsafe {
                            slice::from_raw_parts_mut(
                                self.chunk_buffer.as_mut_ptr() as *mut i16,
                                self.chunk_buffer.len() / 2,
                            )
                        };
                        for i in 0..linear_read_count as usize {
                            assert!(read_coordinate + i < array.len());
                            assert!(write_coordinate + i < length_in_chunk as usize);
                            let val = array[read_coordinate + i];
                            chunk_buffer[write_coordinate + i] = if val.is_nan() {
                                i16::MAX
                            } else {
                                let scaled = val * self.scalefactor;
                                scaled.round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
                            };
                        }
                    }
                    CompressionType::Fpxdec32 => {
                        let chunk_buffer = unsafe {
                            slice::from_raw_parts_mut(
                                self.chunk_buffer.as_mut_ptr() as *mut f32,
                                self.chunk_buffer.len() / 4,
                            )
                        };
                        for i in 0..linear_read_count as usize {
                            assert!(read_coordinate + i < array.len());
                            assert!(write_coordinate + i < length_in_chunk as usize);
                            chunk_buffer[write_coordinate + i] = array[read_coordinate + i];
                        }
                    }
                    CompressionType::P4nzdec256logarithmic => {
                        let chunk_buffer = unsafe {
                            slice::from_raw_parts_mut(
                                self.chunk_buffer.as_mut_ptr() as *mut i16,
                                self.chunk_buffer.len() / 2,
                            )
                        };
                        for i in 0..linear_read_count as usize {
                            assert!(read_coordinate + i < array.len());
                            assert!(write_coordinate + i < length_in_chunk as usize);
                            let val = array[read_coordinate + i];
                            chunk_buffer[write_coordinate + i] = if val.is_nan() {
                                i16::MAX
                            } else {
                                let scaled = (val.log10() + 1.0) * self.scalefactor;
                                scaled.round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
                            };
                        }
                    }
                }

                read_coordinate += linear_read_count as usize - 1;
                write_coordinate += linear_read_count as usize - 1;
                write_coordinate += 1;

                // Move to the next position
                rolling_multiply_target_cube = 1;
                linear_read = true;
                linear_read_count = 1;

                for i in (0..self.dims.len()).rev() {
                    let q_pos = ((read_coordinate as u64 / rolling_multiply_target_cube)
                        % array_dimensions[i]
                        - array_offset[i])
                        / self.chunks[i];
                    let length0 =
                        min((q_pos + 1) * self.chunks[i], array_count[i]) - q_pos * self.chunks[i];

                    // Move forward
                    read_coordinate += rolling_multiply_target_cube as usize;

                    if i == self.dims.len() - 1
                        && !(array_count[i] == length0 && array_dimensions[i] == length0)
                    {
                        linear_read_count = length0;
                        linear_read = false;
                    }

                    if linear_read && array_count[i] == length0 && array_dimensions[i] == length0 {
                        linear_read_count *= length0;
                    } else {
                        linear_read = false;
                    }

                    let q0 = ((read_coordinate as u64 / rolling_multiply_target_cube)
                        % array_dimensions[i]
                        - array_offset[i])
                        % self.chunks[i];
                    if q0 != 0 && q0 != length0 {
                        break; // No overflow in this dimension, break
                    }

                    read_coordinate -= (length0 * rolling_multiply_target_cube) as usize;

                    rolling_multiply_target_cube *= array_dimensions[i];
                    if i == 0 {
                        // All chunks have been read. End of iteration
                        break 'loop_buffer;
                    }
                }
            }

            // Compression and writing to output buffer (same as before)
            let write_length: usize;
            let minimum_buffer: usize;

            match self.compression {
                CompressionType::P4nzdec256 | CompressionType::P4nzdec256logarithmic => {
                    minimum_buffer = p4nenc256_bound(length_in_chunk as usize, 4);
                    assert!(out.buffer.len() - out.write_position as usize >= minimum_buffer);
                    unsafe {
                        delta2d_encode(
                            length_in_chunk as usize / length_last as usize,
                            length_last as usize,
                            self.chunk_buffer.as_mut_ptr() as *mut i16,
                        )
                    };
                    write_length = unsafe {
                        p4nzenc128v16(
                            self.chunk_buffer.as_mut_ptr() as *mut u16,
                            length_in_chunk as usize,
                            out.buffer[out.write_position as usize..].as_mut_ptr(),
                        )
                    };
                }
                CompressionType::Fpxdec32 => {
                    minimum_buffer = p4nenc256_bound(length_in_chunk as usize, 4);
                    assert!(out.buffer.len() - out.write_position as usize >= minimum_buffer);
                    unsafe {
                        delta2d_encode_xor(
                            length_in_chunk as usize / length_last as usize,
                            length_last as usize,
                            self.chunk_buffer.as_mut_ptr() as *mut f32,
                        )
                    };
                    write_length = unsafe {
                        fpxenc32(
                            self.chunk_buffer.as_mut_ptr() as *mut u32,
                            length_in_chunk as usize,
                            out.buffer[out.write_position as usize..].as_mut_ptr(),
                            0,
                        )
                    };
                }
            }

            if self.chunk_index == 0 {
                // Store data start address
                self.chunk_offset_bytes[self.chunk_index] = out.total_bytes_written;
            }

            out.write_position += write_length as u64;
            out.total_bytes_written += write_length as u64;

            // Store chunk offset in LUT
            self.chunk_offset_bytes[self.chunk_index + 1] = out.total_bytes_written;
            self.chunk_index += 1;
            c_offset += 1;

            if c_offset == number_of_chunks_in_array {
                return None;
            }

            // Return to caller if the next chunk would not fit into the buffer
            if out.buffer.len() - out.write_position as usize <= minimum_buffer {
                return Some(c_offset);
            }
        }

        None
    }
}

impl Drop for OmFileEncoder {
    fn drop(&mut self) {
        // No need to manually deallocate the buffer as Vec handles it
    }
}
