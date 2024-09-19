use crate::om::errors::OmFilesRsError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionType {
    P4nzdec256 = 0,
    Fpxdec32 = 1,
    P4nzdec256logarithmic = 3,
    Pico = 4,
}

impl CompressionType {
    pub fn bytes_per_element(&self) -> usize {
        match self {
            CompressionType::P4nzdec256 | CompressionType::P4nzdec256logarithmic => 2,
            CompressionType::Fpxdec32 => 4,
            CompressionType::Pico => 4,
        }
    }
}

impl TryFrom<u8> for CompressionType {
    type Error = OmFilesRsError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressionType::P4nzdec256),
            1 => Ok(CompressionType::Fpxdec32),
            3 => Ok(CompressionType::P4nzdec256logarithmic),
            4 => Ok(CompressionType::Pico),
            _ => Err(OmFilesRsError::InvalidCompressionType),
        }
    }
}

/// For encoding: compression lib read and write more data to buffers
/// https://github.com/powturbo/TurboPFor-Integer-Compression/issues/59
pub fn p4nenc256_bound(n: usize, bytes_per_element: usize) -> usize {
    ((n + 255) / 256 + (n + 32)) * bytes_per_element
}

/// For decoding: compression lib read and write more data to buffers
/// https://github.com/powturbo/TurboPFor-Integer-Compression/issues/59
pub fn p4ndec256_bound(n: usize, bytes_per_element: usize) -> usize {
    n * bytes_per_element + 32 * 4
}
