use crate::compression::CompressionType;
use crate::data_types::{DataType, OmFileArrayDataType, OmFileScalarDataType};
use crate::om::backends::OmFileWriterBackend;
use crate::om::buffered_writer::OmBufferedWriter;
use crate::om::c_defaults::create_encoder;
use crate::om::errors::OmFilesRsError;
use omfileformatc_rs::{
    om_encoder_chunk_buffer_size, om_encoder_compress_chunk, om_encoder_compress_lut,
    om_encoder_compressed_chunk_buffer_size, om_encoder_count_chunks,
    om_encoder_count_chunks_in_array, om_encoder_init, om_encoder_lut_buffer_size, om_header_write,
    om_header_write_size, om_trailer_size, om_trailer_write, om_variable_write_numeric_array,
    om_variable_write_numeric_array_size, om_variable_write_scalar, om_variable_write_scalar_size,
    OmEncoder_t, OmError_t_ERROR_OK, OmOffsetSize_t,
};
use std::borrow::BorrowMut;
use std::os::raw::c_void;

pub struct OmOffsetSize {
    pub offset: OmOffsetSize_t,
}

pub struct OmFileWriter2<FileHandle: OmFileWriterBackend> {
    buffer: OmBufferedWriter<FileHandle>,
}

impl<FileHandle: OmFileWriterBackend> OmFileWriter2<FileHandle> {
    pub fn new(backend: FileHandle, initial_capacity: u64) -> Self {
        Self {
            buffer: OmBufferedWriter::new(backend, initial_capacity as usize),
        }
    }

    pub fn write_header_if_required(&mut self) -> Result<(), OmFilesRsError> {
        if self.buffer.total_bytes_written > 0 {
            return Ok(());
        }
        let size = unsafe { om_header_write_size() };
        self.buffer.reallocate(size as usize)?;
        unsafe {
            om_header_write(self.buffer.buffer_at_write_position().as_mut_ptr() as *mut c_void);
        }
        self.buffer.increment_write_position(size as usize);
        Ok(())
    }

    pub fn write_scalar<T: OmFileScalarDataType>(
        &mut self,
        value: T,
        name: &str,
        children: &[OmOffsetSize],
    ) -> Result<OmOffsetSize, OmFilesRsError> {
        self.write_header_if_required()?;

        assert!(name.len() <= u16::MAX as usize);
        assert!(children.len() <= u32::MAX as usize);

        let size = unsafe {
            om_variable_write_scalar_size(
                name.len() as u16,
                children.len() as u32,
                T::DATA_TYPE_SCALAR.to_c(),
            )
        };

        self.buffer.align_to_64_bytes()?;
        self.buffer.reallocate(size as usize)?;

        let children_offsets: Vec<OmOffsetSize_t> = children.iter().map(|c| c.offset).collect();
        let variable = unsafe {
            om_variable_write_scalar(
                self.buffer.buffer_at_write_position().as_mut_ptr() as *mut c_void,
                self.buffer.total_bytes_written as u64,
                name.len() as u16,
                children.len() as u32,
                children_offsets.as_ptr(),
                name.as_ptr() as *const i8,
                T::DATA_TYPE_SCALAR.to_c(),
                &value as *const T as *const c_void,
            )
        };

        self.buffer.increment_write_position(size as usize);
        Ok(OmOffsetSize { offset: variable })
    }

    pub fn prepare_array<T: OmFileArrayDataType>(
        &mut self,
        dimensions: Vec<u64>,
        chunk_dimensions: Vec<u64>,
        compression: CompressionType,
        scale_factor: f32,
        add_offset: f32,
        lut_chunk_element_count: u64,
    ) -> Result<OmFileWriterArray<FileHandle>, OmFilesRsError> {
        let _ = &self.write_header_if_required()?;

        Ok(OmFileWriterArray::new(
            dimensions,
            chunk_dimensions,
            compression,
            T::DATA_TYPE_ARRAY,
            scale_factor,
            add_offset,
            self.buffer.borrow_mut(),
            lut_chunk_element_count,
        ))
    }

    pub fn write_array(
        &mut self,
        array: OmFileWriterArrayFinalized,
        name: &str,
        children: &[OmOffsetSize],
    ) -> Result<OmOffsetSize, OmFilesRsError> {
        self.write_header_if_required()?;

        assert!(name.len() <= u16::MAX as usize);
        assert_eq!(array.dimensions.len(), array.chunks.len());

        let size = unsafe {
            om_variable_write_numeric_array_size(
                name.len() as u16,
                children.len() as u32,
                array.dimensions.len() as u64,
            )
        };

        self.buffer.align_to_64_bytes()?;
        self.buffer.reallocate(size as usize)?;

        let children_offsets: Vec<OmOffsetSize_t> = children.iter().map(|c| c.offset).collect();
        let variable = unsafe {
            om_variable_write_numeric_array(
                self.buffer.buffer_at_write_position().as_mut_ptr() as *mut c_void,
                self.buffer.total_bytes_written as u64,
                name.len() as u16,
                children.len() as u32,
                children_offsets.as_ptr(),
                name.as_ptr() as *const i8,
                array.data_type.to_c(),
                array.compression.to_c(),
                array.scale_factor,
                array.add_offset,
                array.dimensions.len() as u64,
                array.dimensions.as_ptr(),
                array.chunks.as_ptr(),
                array.lut_size,
                array.lut_offset,
            )
        };

        self.buffer.increment_write_position(size as usize);
        Ok(OmOffsetSize { offset: variable })
    }

    pub fn write_trailer(&mut self, root_variable: OmOffsetSize) -> Result<(), OmFilesRsError> {
        self.write_header_if_required()?;
        self.buffer.align_to_64_bytes()?;

        let size = unsafe { om_trailer_size() };
        self.buffer.reallocate(size as usize)?;
        unsafe {
            om_trailer_write(
                self.buffer.buffer_at_write_position().as_mut_ptr() as *mut c_void,
                root_variable.offset,
            );
        }
        self.buffer.increment_write_position(size as usize);
        self.buffer.write_to_file()
    }
}

pub struct OmFileWriterArray<'a, FileHandle: OmFileWriterBackend> {
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
    buffer: &'a mut OmBufferedWriter<FileHandle>,
}

impl<'a, FileHandle: OmFileWriterBackend> OmFileWriterArray<'a, FileHandle> {
    /// `lut_chunk_element_count` should be 256 for production files.
    pub fn new(
        dimensions: Vec<u64>,
        chunk_dimensions: Vec<u64>,
        compression: CompressionType,
        data_type: DataType,
        scale_factor: f32,
        add_offset: f32,
        buffer: &'a mut OmBufferedWriter<FileHandle>,
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
            buffer,
        }
    }

    /// Compress data and write it to file.
    pub fn write_data(
        &mut self,
        array: &[f32],
        array_dimensions: Option<&[u64]>,
        array_offset: Option<&[u64]>,
        array_count: Option<&[u64]>,
    ) -> Result<(), OmFilesRsError> {
        let array_dimensions = array_dimensions.unwrap_or(&self.dimensions);
        let default_offset = vec![0; array_dimensions.len()];
        let array_offset = array_offset.unwrap_or(default_offset.as_slice());
        let array_count = array_count.unwrap_or(array_dimensions);

        let array_size: u64 = array_dimensions.iter().product::<u64>();
        assert_eq!(array.len() as u64, array_size);

        self.buffer
            .reallocate(self.compressed_chunk_buffer_size as usize * 4)?;

        let number_of_chunks_in_array =
            unsafe { om_encoder_count_chunks_in_array(&mut self.encoder, array_count.as_ptr()) };

        if self.chunk_index == 0 {
            self.look_up_table[self.chunk_index as usize] = self.buffer.total_bytes_written as u64;
        }

        for chunk_offset in 0..number_of_chunks_in_array {
            assert!(self.buffer.remaining_capacity() >= self.compressed_chunk_buffer_size as usize);
            let bytes_written = unsafe {
                om_encoder_compress_chunk(
                    &mut self.encoder,
                    array.as_ptr() as *const c_void,
                    array_dimensions.as_ptr(),
                    array_offset.as_ptr(),
                    array_count.as_ptr(),
                    self.chunk_index,
                    chunk_offset,
                    self.buffer.buffer_at_write_position().as_mut_ptr(),
                    self.chunk_buffer.as_mut_ptr(),
                )
            };

            self.buffer.increment_write_position(bytes_written as usize);

            self.look_up_table[(self.chunk_index + 1) as usize] =
                self.buffer.total_bytes_written as u64;
            self.chunk_index += 1;

            if self.buffer.remaining_capacity() < self.compressed_chunk_buffer_size as usize {
                self.buffer.write_to_file()?;
            }
        }

        Ok(())
    }

    /// Compress the lookup table and write it to the output buffer.
    pub fn write_lut(&mut self) -> u64 {
        let buffer_size = unsafe {
            om_encoder_lut_buffer_size(
                &mut self.encoder,
                self.look_up_table.as_ptr(),
                self.look_up_table.len() as u64,
            )
        };
        self.buffer
            .reallocate(buffer_size as usize)
            .expect("Failed to reallocate buffer");

        let compressed_lut_size = unsafe {
            om_encoder_compress_lut(
                &mut self.encoder,
                self.look_up_table.as_ptr(),
                self.look_up_table.len() as u64,
                self.buffer.buffer_at_write_position().as_mut_ptr(),
                buffer_size,
            )
        };

        self.buffer
            .increment_write_position(compressed_lut_size as usize);
        compressed_lut_size as u64
    }

    /// Finalize the array and return the finalized struct.
    pub fn finalize(mut self) -> OmFileWriterArrayFinalized {
        let lut_offset = self.buffer.total_bytes_written as u64;
        let lut_size = self.write_lut();

        OmFileWriterArrayFinalized {
            scale_factor: self.scale_factor,
            add_offset: self.add_offset,
            compression: self.compression,
            data_type: self.data_type,
            dimensions: self.dimensions.clone(),
            chunks: self.chunks.clone(),
            lut_size,
            lut_offset,
        }
    }
}

impl<FileHandle: OmFileWriterBackend> Drop for OmFileWriterArray<'_, FileHandle> {
    fn drop(&mut self) {}
}

pub struct OmFileWriterArrayFinalized {
    pub scale_factor: f32,
    pub add_offset: f32,
    pub compression: CompressionType,
    pub data_type: DataType,
    pub dimensions: Vec<u64>,
    pub chunks: Vec<u64>,
    pub lut_size: u64,
    pub lut_offset: u64,
}
