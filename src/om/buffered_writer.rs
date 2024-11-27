use crate::om::backends::OmFileWriterBackend;
use crate::om::errors::OmFilesRsError;

/// All data is written to a buffer before flushed to a backend
pub struct OmBufferedWriter<FileHandle: OmFileWriterBackend> {
    /// All data is written to this buffer
    buffer: Vec<u8>,
    /// The final backing store to write data to
    pub backend: FileHandle,
    /// Current write position in buffer
    pub write_position: usize,
    /// Total bytes written including flushed data
    pub total_bytes_written: usize,
    /// Initial capacity for reallocation sizing
    pub initial_capacity: usize,
}

impl<FileHandle: OmFileWriterBackend> OmBufferedWriter<FileHandle> {
    pub fn new(backend: FileHandle, initial_capacity: usize) -> Self {
        Self {
            buffer: vec![0; initial_capacity],
            backend,
            write_position: 0,
            total_bytes_written: 0,
            initial_capacity,
        }
    }

    pub fn increment_write_position(&mut self, bytes: usize) {
        self.write_position += bytes;
        self.total_bytes_written += bytes;
    }

    pub fn reset_write_position(&mut self) {
        self.write_position = 0;
    }

    /// Add empty space if required to align to 64 bits
    pub fn align_to_64_bytes(&mut self) -> Result<(), OmFilesRsError> {
        let bytes_to_pad = 8 - self.total_bytes_written % 8;
        if bytes_to_pad == 8 {
            return Ok(());
        }
        self.reallocate(bytes_to_pad)?;

        // Zero-fill padding bytes
        for _ in 0..bytes_to_pad {
            self.buffer[self.write_position] = 0;
            self.increment_write_position(1);
        }
        Ok(())
    }

    /// How many bytes are left in the write buffer
    pub fn remaining_capacity(&self) -> usize {
        self.buffer.capacity() - self.write_position
    }

    /// Get a mutable slice to the current write position
    pub fn buffer_at_write_position(&mut self) -> &mut [u8] {
        &mut self.buffer[self.write_position..]
    }

    /// Get current buffer contents
    pub fn buffer(&self) -> &[u8] {
        &self.buffer[..self.write_position]
    }

    /// Ensure the buffer has at least a minimum capacity
    pub fn reallocate(&mut self, minimum_capacity: usize) -> Result<(), OmFilesRsError> {
        if self.remaining_capacity() >= minimum_capacity {
            return Ok(());
        }

        self.write_to_file()?;

        if self.buffer.capacity() >= minimum_capacity {
            return Ok(());
        }

        // Calculate new capacity as multiple of initial capacity
        let new_capacity = ((minimum_capacity + self.initial_capacity - 1) / self.initial_capacity)
            * self.initial_capacity;

        // Resize buffer with zeros
        self.buffer.resize(new_capacity, 0);
        self.buffer.shrink_to(new_capacity);

        Ok(())
    }

    /// Write buffer to file
    pub fn write_to_file(&mut self) -> Result<(), OmFilesRsError> {
        if self.write_position == 0 {
            return Ok(());
        }

        self.backend.write(&self.buffer[..self.write_position])?;
        self.reset_write_position();

        // Clear buffer contents
        self.buffer.fill(0);

        Ok(())
    }
}

impl<FileHandle: OmFileWriterBackend> Drop for OmBufferedWriter<FileHandle> {
    fn drop(&mut self) {
        // Vec handles cleanup automatically
    }
}
