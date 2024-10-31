use crate::compression::CompressionType;
use crate::data_types::DataType;
// use crate::datatypes::DataType;
use crate::om::backends::OmFileWriterBackend;
use crate::om::decoder::create_encoder;
use crate::om::errors::OmFilesRsError;
use crate::om::omfile_json::{OmFileJSON, OmFileJSONVariable};
use crate::om::write_buffer::OmWriteBuffer;
use omfileformatc_rs::{
    OmEncoder_chunkBufferSize, OmEncoder_compressChunk, OmEncoder_compressLut,
    OmEncoder_compressedChunkBufferSize, OmEncoder_countChunks, OmEncoder_countChunksInArray,
    OmEncoder_init, OmEncoder_lutBufferSize, OmEncoder_t, OmError_t_ERROR_OK,
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
        buffer.reallocate(json.len());
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
    look_up_table: Vec<usize>,
    encoder: OmEncoder_t,
    chunk_index: usize,
    scale_factor: f32,
    add_offset: f32,
    compression: CompressionType,
    data_type: DataType,
    dimensions: Vec<usize>,
    chunks: Vec<usize>,
    compressed_chunk_buffer_size: usize,
    chunk_buffer: Vec<u8>,
}

impl OmFileWriterArray {
    /// `lut_chunk_element_count` should be 256 for production files.
    pub fn new(
        dimensions: Vec<usize>,
        chunk_dimensions: Vec<usize>,
        compression: CompressionType,
        data_type: DataType,
        scale_factor: f32,
        add_offset: f32,
        lut_chunk_element_count: usize,
    ) -> Self {
        assert_eq!(dimensions.len(), chunk_dimensions.len());

        let chunks = chunk_dimensions;

        let mut encoder = create_encoder();
        let error = unsafe {
            OmEncoder_init(
                &mut encoder,
                scale_factor,
                add_offset,
                compression.to_c(),
                data_type.to_c(),
                dimensions.as_ptr(),
                chunks.as_ptr(),
                dimensions.len(),
                lut_chunk_element_count,
            )
        };
        assert_eq!(error, OmError_t_ERROR_OK, "OmEncoder_init failed");

        let n_chunks = unsafe { OmEncoder_countChunks(&encoder) } as usize;
        let compressed_chunk_buffer_size =
            unsafe { OmEncoder_compressedChunkBufferSize(&encoder) } as usize;
        let chunk_buffer_size = unsafe { OmEncoder_chunkBufferSize(&encoder) } as usize;

        let chunk_buffer = vec![0u8; chunk_buffer_size];
        let look_up_table = vec![0usize; n_chunks + 1];

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
        array_dimensions: &[usize],
        array_read: &[Range<usize>],
        file_handle: &mut FileHandle,
        buffer: &mut OmWriteBuffer,
    ) -> Result<(), OmFilesRsError> {
        let array_size: usize = array_dimensions.iter().product::<usize>();
        assert_eq!(array.len(), array_size);

        let array_offset: Vec<usize> = array_read.iter().map(|r| r.start).collect();
        let array_count: Vec<usize> = array_read.iter().map(|r| (r.end - r.start)).collect();

        buffer.reallocate(self.compressed_chunk_buffer_size * 4);

        let number_of_chunks_in_array =
            unsafe { OmEncoder_countChunksInArray(&mut self.encoder, array_count.as_ptr()) }
                as usize;

        if self.chunk_index == 0 {
            self.look_up_table[self.chunk_index] = buffer.total_bytes_written;
        }

        for chunk_offset in 0..number_of_chunks_in_array {
            assert!(buffer.remaining_capacity() >= self.compressed_chunk_buffer_size);
            let bytes_written = unsafe {
                OmEncoder_compressChunk(
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
            } as usize;

            buffer.increment_write_position(bytes_written);

            self.look_up_table[self.chunk_index + 1] = buffer.total_bytes_written;
            self.chunk_index += 1;

            if buffer.remaining_capacity() < self.compressed_chunk_buffer_size {
                buffer.write_to_file(file_handle)?;
            }
        }

        Ok(())
    }

    /// Compress the lookup table and write it to the output buffer.
    pub fn write_lut(&mut self, buffer: &mut OmWriteBuffer) -> usize {
        let buffer_size = unsafe {
            OmEncoder_lutBufferSize(
                &mut self.encoder,
                self.look_up_table.as_ptr(),
                self.look_up_table.len(),
            )
        } as usize;
        buffer.reallocate(buffer_size);

        let compressed_lut_size = unsafe {
            OmEncoder_compressLut(
                &mut self.encoder,
                self.look_up_table.as_ptr(),
                self.look_up_table.len(),
                buffer.buffer_at_write_position().as_mut_ptr(),
                buffer_size,
            )
        } as usize;

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
            compression: self.compression.to_c(),
            data_type: self.data_type.to_c(),
            lut_offset,
            lut_size,
        }
    }
}

impl Drop for OmFileWriterArray {
    fn drop(&mut self) {}
}
