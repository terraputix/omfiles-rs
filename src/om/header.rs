use crate::{compression::CompressionType, om::errors::OmFilesRsError};

#[repr(C)]
pub struct OmHeader {
    pub magic_number1: u8,
    pub magic_number2: u8,
    pub version: u8,
    pub compression: CompressionType,
    pub scale_factor: f32,
    pub dim0: u64,
    pub dim1: u64,
    pub chunk0: u64,
    pub chunk1: u64,
}

impl OmHeader {
    pub const MAGIC_NUMBER1: u8 = 79;
    pub const MAGIC_NUMBER2: u8 = 77;
    pub const VERSION: u8 = 2;
    pub const LENGTH: usize = 40;

    /// Create a new OmHeader from a slice of bytes.
    ///
    /// This implementation returns an owned value because the header
    /// is small an we can just copy it for safety.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, OmFilesRsError> {
        if bytes.len() != Self::LENGTH {
            return Err(OmFilesRsError::InvalidHeaderLength);
        }

        let magic_number1 = bytes[0];
        let magic_number2 = bytes[1];
        let version = bytes[2];
        let compression = CompressionType::try_from(bytes[3])?;

        let scale_factor = f32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let dim0 = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let dim1 = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let chunk0 = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let chunk1 = u64::from_le_bytes(bytes[32..40].try_into().unwrap());

        let value = Self {
            magic_number1,
            magic_number2,
            version,
            compression,
            scale_factor,
            dim0,
            dim1,
            chunk0,
            chunk1,
        };

        if value.magic_number1 != Self::MAGIC_NUMBER1 || value.magic_number2 != Self::MAGIC_NUMBER2
        {
            return Err(OmFilesRsError::NotAOmFile);
        }

        Ok(value)
    }

    pub fn as_bytes(self) -> [u8; Self::LENGTH] {
        let mut bytes = [0u8; Self::LENGTH];
        bytes[0] = self.magic_number1;
        bytes[1] = self.magic_number2;
        bytes[2] = self.version;
        bytes[3] = self.compression as u8;

        let scale_factor_bytes = self.scale_factor.to_le_bytes();
        let dim0_bytes = self.dim0.to_le_bytes();
        let dim1_bytes = self.dim1.to_le_bytes();
        let chunk0_bytes = self.chunk0.to_le_bytes();
        let chunk1_bytes = self.chunk1.to_le_bytes();

        bytes[4..8].copy_from_slice(&scale_factor_bytes);
        bytes[8..16].copy_from_slice(&dim0_bytes);
        bytes[16..24].copy_from_slice(&dim1_bytes);
        bytes[24..32].copy_from_slice(&chunk0_bytes);
        bytes[32..40].copy_from_slice(&chunk1_bytes);

        bytes
    }
}
