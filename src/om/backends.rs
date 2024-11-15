use omfileformatc_rs::{
    OmDecoder_decodeChunks, OmDecoder_nexDataRead, OmDecoder_nextIndexRead, OmDecoder_t,
    OmError_t_ERROR_OK,
};

use crate::data_types::OmFileDataType;
use crate::om::c_defaults::new_data_read;
use crate::om::errors::OmFilesRsError;
use std::os::raw::c_void;

use super::c_defaults::new_index_read;

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

    fn decode<OmType: OmFileDataType>(
        &self,
        decoder: &OmDecoder_t,
        into: &mut [OmType],
        chunk_buffer: &mut [u8],
    ) -> Result<(), OmFilesRsError> {
        let mut index_read = new_index_read(decoder);
        unsafe {
            // Loop over index blocks and read index data
            while OmDecoder_nextIndexRead(decoder, &mut index_read) {
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
