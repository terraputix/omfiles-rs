use crate::om::backends::OmFileWriterBackend;
use crate::om::errors::OmFilesRsError;

pub struct OmWriteBuffer {
    buffer: Vec<u8>,
    write_position: usize,
    pub total_bytes_written: usize,
}

impl OmWriteBuffer {
    /// Creates a new `OmWriteBuffer` with the specified capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![0u8; capacity],
            write_position: 0,
            total_bytes_written: 0,
        }
    }

    /// Increments the write position by the given number of bytes.
    pub fn increment_write_position(&mut self, bytes: usize) {
        self.write_position += bytes;
        self.total_bytes_written += bytes;
    }

    /// Resets the write position to zero.
    pub fn reset_write_position(&mut self) {
        self.write_position = 0;
    }

    /// Returns the remaining capacity in the buffer.
    pub fn remaining_capacity(&self) -> usize {
        self.buffer.len() - self.write_position
    }

    /// Returns a mutable slice starting at the current write position.
    pub fn buffer_at_write_position(&mut self) -> &mut [u8] {
        &mut self.buffer[self.write_position..]
    }

    /// Ensures the buffer has at least the specified minimum capacity.
    pub fn reallocate(&mut self, minimum_capacity: usize) {
        let needed_capacity = self.write_position + minimum_capacity;
        if self.buffer.len() < needed_capacity {
            self.buffer.resize(needed_capacity, 0);
        }
    }

    /// Writes a `u8` value to the buffer.
    pub fn write_u8(&mut self, value: u8) {
        self.reallocate(1);
        if self.write_position < self.buffer.len() {
            self.buffer[self.write_position] = value;
        } else {
            self.buffer.push(value);
        }
        self.increment_write_position(1);
    }

    /// Writes a slice of bytes to the buffer.
    pub fn write_bytes(&mut self, data: &[u8]) {
        let len = data.len();
        self.reallocate(len);
        if self.write_position + len <= self.buffer.len() {
            self.buffer[self.write_position..self.write_position + len].copy_from_slice(data);
        } else {
            self.buffer.extend_from_slice(data);
        }
        self.increment_write_position(len);
    }

    /// Writes an `i64` value to the buffer in little-endian order.
    pub fn write_u64_le(&mut self, value: u64) {
        let bytes = value.to_le_bytes();
        self.write_bytes(&bytes);
    }

    /// Writes the buffer contents to the file and resets the write position.
    pub fn write_to_file<FileHandle: OmFileWriterBackend>(
        &mut self,
        file_handle: &mut FileHandle,
    ) -> Result<(), OmFilesRsError> {
        let data = &self.buffer[..self.write_position];
        file_handle.write(data)?;
        self.reset_write_position();
        Ok(())
    }
}
