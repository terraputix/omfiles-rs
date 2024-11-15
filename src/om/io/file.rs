use std::fs::File;
// use std::io::SeekFrom;
use std::io::{Seek, SeekFrom, Write};

use crate::om::{backends::OmFileWriterBackend, errors::OmFilesRsError};

// TODO: fix error names
impl OmFileWriterBackend for File {
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
