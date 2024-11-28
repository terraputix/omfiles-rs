#[derive(Debug, PartialEq)]
pub enum OmFilesRsError {
    CannotOpenFile {
        filename: String,
        errno: i32,
        error: String,
    },
    CannotOpenFileErrno {
        errno: i32,
        error: String,
    },
    ChunkHasWrongNumberOfElements,
    DimensionOutOfBounds {
        range: std::ops::Range<usize>,
        allowed: usize,
    },
    ChunkDimensionIsSmallerThanOverallDim,
    DimensionMustBeLargerThan0,
    FileExistsAlready {
        filename: String,
    },
    InvalidCompressionType,
    InvalidDataType,
    DecoderError(String),
    NotAnOmFile,
}

impl std::fmt::Display for OmFilesRsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OmFilesRsError::CannotOpenFile {
                filename,
                errno,
                error,
            } => {
                write!(
                    f,
                    "Cannot open file '{}': errno {}, error: {}",
                    filename, errno, error
                )
            }
            OmFilesRsError::CannotOpenFileErrno { errno, error } => {
                write!(f, "Cannot open file: errno {}, error: {}", errno, error)
            }
            OmFilesRsError::ChunkHasWrongNumberOfElements => {
                write!(f, "Chunk has wrong number of elements")
            }
            OmFilesRsError::DimensionOutOfBounds { range, allowed } => {
                write!(
                    f,
                    "Dimension out of bounds: range {:?}, allowed {}",
                    range, allowed
                )
            }
            OmFilesRsError::ChunkDimensionIsSmallerThanOverallDim => {
                write!(f, "Chunk dimension is smaller than overall dimension")
            }
            OmFilesRsError::DimensionMustBeLargerThan0 => {
                write!(f, "Dimension must be larger than 0")
            }
            OmFilesRsError::FileExistsAlready { filename } => {
                write!(f, "File '{}' already exists", filename)
            }
            OmFilesRsError::InvalidCompressionType => {
                write!(f, "Invalid compression type")
            }
            OmFilesRsError::InvalidDataType => {
                write!(f, "Invalid data type")
            }
            OmFilesRsError::DecoderError(e) => {
                write!(f, "Decoder error {}", e)
            }
            OmFilesRsError::NotAnOmFile => {
                write!(f, "Not an OM file")
            }
        }
    }
}

impl std::error::Error for OmFilesRsError {}
