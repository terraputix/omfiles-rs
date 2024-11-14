use crate::om::{
    backends::{OmFileReaderBackend, OmFileWriterBackend},
    errors::OmFilesRsError,
};

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
