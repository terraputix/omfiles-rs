use crate::backend::mmapfile::{MAdvice, MmapFile, MmapType};
use crate::core::c_defaults::{c_error_string, new_data_read, new_index_read};
use crate::core::data_types::OmFileArrayDataType;
use crate::errors::OmFilesRsError;
use omfileformatc_rs::{
    om_decoder_decode_chunks, om_decoder_next_data_read, om_decoder_next_index_read, OmDecoder_t,
    OmError_t_ERROR_OK,
};
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::os::raw::c_void;

pub trait OmFileWriterBackend {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesRsError>;
    fn write_at(&mut self, data: &[u8], offset: usize) -> Result<(), OmFilesRsError>;
    fn synchronize(&self) -> Result<(), OmFilesRsError>;
}

pub trait OmFileReaderBackend {
    /// Length in bytes
    fn count(&self) -> usize;
    fn needs_prefetch(&self) -> bool;
    fn prefetch_data(&self, offset: usize, count: usize);
    fn pre_read(&self, offset: usize, count: usize) -> Result<(), OmFilesRsError>;
    fn get_bytes(&self, offset: u64, count: u64) -> Result<&[u8], OmFilesRsError>;

    fn decode<OmType: OmFileArrayDataType>(
        &self,
        decoder: &OmDecoder_t,
        into: &mut [OmType],
        chunk_buffer: &mut [u8],
    ) -> Result<(), OmFilesRsError> {
        let mut index_read = new_index_read(decoder);
        unsafe {
            // Loop over index blocks and read index data
            while om_decoder_next_index_read(decoder, &mut index_read) {
                let index_data = self.get_bytes(index_read.offset, index_read.count)?;

                let mut data_read = new_data_read(&index_read);

                let mut error = OmError_t_ERROR_OK;

                // Loop over data blocks and read compressed data chunks
                while om_decoder_next_data_read(
                    decoder,
                    &mut data_read,
                    index_data.as_ptr() as *const c_void,
                    index_read.count,
                    &mut error,
                ) {
                    let data_data = self.get_bytes(data_read.offset, data_read.count)?;

                    if !om_decoder_decode_chunks(
                        decoder,
                        data_read.chunkIndex,
                        data_data.as_ptr() as *const c_void,
                        data_read.count,
                        into.as_mut_ptr() as *mut c_void,
                        chunk_buffer.as_mut_ptr() as *mut c_void,
                        &mut error,
                    ) {
                        let error_string = c_error_string(error);

                        panic!("OmDecoder: {:}", &error_string);
                    }
                }
                if error != OmError_t_ERROR_OK {
                    let error_string = c_error_string(error);
                    panic!("OmDecoder: {:}", &error_string);
                }
            }
        }
        Ok(())
    }
}

fn map_io_error(e: std::io::Error) -> OmFilesRsError {
    OmFilesRsError::FileWriterError {
        errno: e.raw_os_error().unwrap_or(0),
        error: e.to_string(),
    }
}

impl OmFileWriterBackend for &File {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesRsError> {
        self.write_all(data).map_err(|e| map_io_error(e))?;
        Ok(())
    }

    fn write_at(&mut self, data: &[u8], offset: usize) -> Result<(), OmFilesRsError> {
        self.seek(SeekFrom::Start(offset as u64))
            .map_err(|e| map_io_error(e))?;
        self.write_all(data).map_err(|e| map_io_error(e))?;
        Ok(())
    }

    fn synchronize(&self) -> Result<(), OmFilesRsError> {
        self.sync_all().map_err(|e| map_io_error(e))?;
        Ok(())
    }
}

impl OmFileReaderBackend for MmapFile {
    fn count(&self) -> usize {
        self.data.len()
    }

    fn needs_prefetch(&self) -> bool {
        true
    }

    fn prefetch_data(&self, offset: usize, count: usize) {
        self.prefetch_data_advice(offset, count, MAdvice::WillNeed);
    }

    fn pre_read(&self, _offset: usize, _count: usize) -> Result<(), OmFilesRsError> {
        // No-op for mmaped file
        Ok(())
    }

    fn get_bytes(&self, offset: u64, count: u64) -> Result<&[u8], OmFilesRsError> {
        let index_range = (offset as usize)..(offset + count) as usize;
        match self.data {
            MmapType::ReadOnly(ref mmap) => Ok(&mmap[index_range]),
            MmapType::ReadWrite(ref mmap_mut) => Ok(&mmap_mut[index_range]),
        }
    }
}

#[derive(Debug)]
pub struct InMemoryBackend {
    data: Vec<u8>,
}

impl InMemoryBackend {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl OmFileWriterBackend for &mut InMemoryBackend {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesRsError> {
        self.data.extend_from_slice(data);
        Ok(())
    }

    fn write_at(&mut self, data: &[u8], offset: usize) -> Result<(), OmFilesRsError> {
        self.data.reserve(offset + data.len());
        let dst = &mut self.data[offset..offset + data.len()];
        dst.copy_from_slice(data);
        Ok(())
    }

    fn synchronize(&self) -> Result<(), OmFilesRsError> {
        // No-op for in-memory backend
        Ok(())
    }
}

impl OmFileReaderBackend for InMemoryBackend {
    fn count(&self) -> usize {
        self.data.len()
    }

    fn needs_prefetch(&self) -> bool {
        false
    }

    fn prefetch_data(&self, _offset: usize, _count: usize) {
        // No-op for in-memory backend
    }

    fn pre_read(&self, _offset: usize, _count: usize) -> Result<(), OmFilesRsError> {
        // No-op for in-memory backend
        Ok(())
    }

    fn get_bytes(&self, offset: u64, count: u64) -> Result<&[u8], OmFilesRsError> {
        let index_range = (offset as usize)..(offset + count) as usize;
        Ok(&self.data[index_range])
    }
}
