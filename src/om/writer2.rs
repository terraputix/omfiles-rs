use crate::compression::CompressionType;
use crate::data_types::DataType;
// use crate::datatypes::DataType;
use crate::om::backends::OmFileWriterBackend;
use crate::om::c_defaults::create_encoder;
use crate::om::errors::OmFilesRsError;
use crate::om::omfile_json::{OmFileJSON, OmFileJSONVariable};
use crate::om::write_buffer::OmWriteBuffer;
use omfileformatc_rs::{
    om_encoder_chunk_buffer_size, om_encoder_compress_chunk, om_encoder_compress_lut,
    om_encoder_compressed_chunk_buffer_size, om_encoder_count_chunks,
    om_encoder_count_chunks_in_array, om_encoder_init, om_encoder_lut_buffer_size, OmEncoder_t,
    OmError_t_ERROR_OK,
};
use std::ops::Range;
use std::os::raw::c_void;

use super::header::OmHeader;

pub struct OmFileWriter2;

impl OmFileWriter2 {
    /// Write header. Only magic number and version 3
    pub fn write_header(buffer: &mut OmWriteBuffer) {
        buffer.reallocate(3);
        buffer.write_u8(OmHeader::MAGIC_NUMBER1);
        buffer.write_u8(OmHeader::MAGIC_NUMBER2);
        buffer.write_u8(3); // version
    }

    /// Write trailer with JSON metadata
    pub fn write_trailer(
        buffer: &mut OmWriteBuffer,
        meta: &OmFileJSON,
    ) -> Result<(), serde_json::Error> {
        // Serialize and write JSON
        let json = serde_json::to_vec(meta)?;
        buffer.reallocate(json.len() as u64);
        buffer.write_bytes(&json);

        // Write length of JSON
        buffer.reallocate(8);
        let json_length = json.len() as u64;
        buffer.write_u64_le(json_length);

        Ok(())
    }
}

/// Compress a single variable inside an om file.
pub struct OmFileWriterArray {
    look_up_table: Vec<u64>,
    encoder: OmEncoder_t,
    chunk_index: u64,
    scale_factor: f32,
    add_offset: f32,
    compression: CompressionType,
    data_type: DataType,
    dimensions: Vec<u64>,
    chunks: Vec<u64>,
    compressed_chunk_buffer_size: u64,
    chunk_buffer: Vec<u8>,
}

impl OmFileWriterArray {
    /// `lut_chunk_element_count` should be 256 for production files.
    pub fn new(
        dimensions: Vec<u64>,
        chunk_dimensions: Vec<u64>,
        compression: CompressionType,
        data_type: DataType,
        scale_factor: f32,
        add_offset: f32,
        lut_chunk_element_count: u64,
    ) -> Self {
        assert_eq!(dimensions.len(), chunk_dimensions.len());

        let chunks = chunk_dimensions;

        let mut encoder = create_encoder();
        let error = unsafe {
            om_encoder_init(
                &mut encoder,
                scale_factor,
                add_offset,
                compression.to_c(),
                data_type.to_c(),
                dimensions.as_ptr(),
                chunks.as_ptr(),
                dimensions.len() as u64,
                lut_chunk_element_count,
            )
        };
        assert_eq!(error, OmError_t_ERROR_OK, "OmEncoder_init failed");

        let n_chunks = unsafe { om_encoder_count_chunks(&encoder) } as usize;
        let compressed_chunk_buffer_size =
            unsafe { om_encoder_compressed_chunk_buffer_size(&encoder) };
        let chunk_buffer_size = unsafe { om_encoder_chunk_buffer_size(&encoder) } as usize;

        let chunk_buffer = vec![0u8; chunk_buffer_size];
        let look_up_table = vec![0u64; n_chunks + 1];

        Self {
            look_up_table,
            encoder,
            chunk_index: 0,
            scale_factor,
            add_offset,
            compression,
            data_type,
            dimensions,
            chunks,
            compressed_chunk_buffer_size,
            chunk_buffer,
        }
    }

    /// Compress data and write it to file.
    pub fn write_data<FileHandle: OmFileWriterBackend>(
        &mut self,
        array: &[f32],
        array_dimensions: &[u64],
        array_read: &[Range<usize>],
        file_handle: &mut FileHandle,
        buffer: &mut OmWriteBuffer,
    ) -> Result<(), OmFilesRsError> {
        let array_size: u64 = array_dimensions.iter().product::<u64>();
        assert_eq!(array.len() as u64, array_size);

        let array_offset: Vec<u64> = array_read.iter().map(|r| r.start as u64).collect();
        let array_count: Vec<u64> = array_read
            .iter()
            .map(|r| (r.end as u64 - r.start as u64))
            .collect();

        buffer.reallocate(self.compressed_chunk_buffer_size * 4);

        let number_of_chunks_in_array =
            unsafe { om_encoder_count_chunks_in_array(&mut self.encoder, array_count.as_ptr()) };

        if self.chunk_index == 0 {
            self.look_up_table[self.chunk_index as usize] = buffer.total_bytes_written;
        }

        for chunk_offset in 0..number_of_chunks_in_array {
            assert!(buffer.remaining_capacity() >= self.compressed_chunk_buffer_size);
            let bytes_written = unsafe {
                om_encoder_compress_chunk(
                    &mut self.encoder,
                    array.as_ptr() as *const c_void,
                    array_dimensions.as_ptr(),
                    array_offset.as_ptr(),
                    array_count.as_ptr(),
                    self.chunk_index,
                    chunk_offset,
                    buffer.buffer_at_write_position().as_mut_ptr(),
                    self.chunk_buffer.as_mut_ptr(),
                )
            };

            buffer.increment_write_position(bytes_written);

            self.look_up_table[(self.chunk_index + 1) as usize] = buffer.total_bytes_written;
            self.chunk_index += 1;

            if buffer.remaining_capacity() < self.compressed_chunk_buffer_size {
                buffer.write_to_file(file_handle)?;
            }
        }

        Ok(())
    }

    /// Compress the lookup table and write it to the output buffer.
    pub fn write_lut(&mut self, buffer: &mut OmWriteBuffer) -> u64 {
        let buffer_size = unsafe {
            om_encoder_lut_buffer_size(
                &mut self.encoder,
                self.look_up_table.as_ptr(),
                self.look_up_table.len() as u64,
            )
        };
        buffer.reallocate(buffer_size);

        let compressed_lut_size = unsafe {
            om_encoder_compress_lut(
                &mut self.encoder,
                self.look_up_table.as_ptr(),
                self.look_up_table.len() as u64,
                buffer.buffer_at_write_position().as_mut_ptr(),
                buffer_size,
            )
        };

        buffer.increment_write_position(compressed_lut_size);
        compressed_lut_size
    }

    /// Compress the lookup table, write it to the output buffer, and return the JSON metadata.
    pub fn compress_lut_and_return_meta(
        &mut self,
        buffer: &mut OmWriteBuffer,
    ) -> OmFileJSONVariable {
        let lut_offset = buffer.total_bytes_written;
        let lut_size = self.write_lut(buffer);

        OmFileJSONVariable {
            name: None,
            dimensions: self.dimensions.clone(),
            chunks: self.chunks.clone(),
            dimension_names: None,
            scalefactor: self.scale_factor,
            add_offset: self.add_offset,
            compression: self.compression,
            data_type: self.data_type,
            lut_offset,
            lut_size,
        }
    }
}

impl Drop for OmFileWriterArray {
    fn drop(&mut self) {}
}
