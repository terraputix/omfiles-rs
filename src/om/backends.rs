use omfileformatc_rs::{
    OmDecoder_decodeChunks, OmDecoder_nexDataRead, OmDecoder_nextIndexRead, OmDecoder_t,
    OmError_t_ERROR_OK,
};

use crate::om::decoder::new_data_read;
use crate::om::errors::OmFilesRsError;
use crate::om::mmapfile::MmapType;
use crate::om::mmapfile::{MAdvice, MmapFile};
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::os::raw::c_void;

use super::decoder::new_index_read;

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
    fn get_bytes(&self, offset: usize, count: usize) -> Result<&[u8], OmFilesRsError>;

    fn decode(
        &self,
        decoder: &OmDecoder_t,
        into: &mut [f32],
        chunk_buffer: &mut [u8],
    ) -> Result<(), OmFilesRsError> {
        println!("decoder: {:?}", decoder);

        let mut index_read = new_index_read(decoder);
        unsafe {
            // Loop over index blocks and read index data
            while OmDecoder_nextIndexRead(decoder, &mut index_read) {
                println!("index_read: {:?}", index_read);
                let index_data =
                    self.get_bytes(index_read.offset as usize, index_read.count as usize)?;

                let mut data_read = new_data_read(&index_read);

                let mut error = OmError_t_ERROR_OK;

                // Loop over data blocks and read compressed data chunks
                while OmDecoder_nexDataRead(
                    decoder,
                    &mut data_read,
                    index_data.as_ptr() as *const c_void, // Urgh!
                    index_read.count,
                    &mut error,
                ) {
                    println!("data_read: {:?}", data_read);
                    let data_data =
                        self.get_bytes(data_read.offset as usize, data_read.count as usize)?;

                    OmDecoder_decodeChunks(
                        decoder,
                        data_read.chunkIndex,
                        data_data.as_ptr() as *const c_void, // Urgh!
                        data_read.count,
                        into.as_mut_ptr() as *mut c_void, // Urgh!
                        chunk_buffer.as_mut_ptr() as *mut c_void, // Urgh!
                        &mut error,
                    );
                }
            }
        }
        Ok(())
    }
}

// TODO: fix error names
impl OmFileWriterBackend for &mut File {
    fn write(&mut self, data: &[u8]) -> Result<(), OmFilesRsError> {
        self.write_all(data)
            .map_err(|e| OmFilesRsError::CannotOpenFileErrno {
                errno: e.raw_os_error().unwrap_or(0),
                error: e.to_string(),
            })?;
        Ok(())
    }

    fn write_at(&mut self, data: &[u8], offset: usize) -> Result<(), OmFilesRsError> {
        self.seek(SeekFrom::Start(offset as u64)).map_err(|e| {
            OmFilesRsError::CannotOpenFileErrno {
                errno: e.raw_os_error().unwrap_or(0),
                error: e.to_string(),
            }
        })?;
        self.write_all(data)
            .map_err(|e| OmFilesRsError::CannotOpenFileErrno {
                errno: e.raw_os_error().unwrap_or(0),
                error: e.to_string(),
            })?;
        Ok(())
    }

    fn synchronize(&self) -> Result<(), OmFilesRsError> {
        self.sync_all()
            .map_err(|e| OmFilesRsError::CannotOpenFileErrno {
                errno: e.raw_os_error().unwrap_or(0),
                error: e.to_string(),
            })?;
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

    fn get_bytes(&self, offset: usize, count: usize) -> Result<&[u8], OmFilesRsError> {
        match self.data {
            MmapType::ReadOnly(ref mmap) => Ok(&mmap[offset..offset + count]),
            MmapType::ReadWrite(ref mmap_mut) => Ok(&mmap_mut[offset..offset + count]),
        }
    }
}

#[derive(Debug)]
pub struct InMemoryBackend {
    data: Vec<u8>,
}

impl InMemoryBackend {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data: data }
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

    fn get_bytes(&self, offset: usize, count: usize) -> Result<&[u8], OmFilesRsError> {
        Ok(&self.data[offset..offset + count])
    }
}
