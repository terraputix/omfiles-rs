#[derive(Debug, PartialEq)]
pub enum OmFilesRsError {
    CannotOpenFile {
        filename: String,
        errno: i32,
        error: String,
    },
    CannotCreateFile {
        filename: String,
        errno: i32,
        error: String,
    },
    CannotTruncateFile {
        filename: String,
        errno: i32,
        error: String,
    },
    CannotOpenFileErrno {
        errno: i32,
        error: String,
    },
    CannotMoveFile {
        from: String,
        to: String,
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
    NotAOmFile,
    InvalidHeaderLength,
    FileExistsAlready {
        filename: String,
    },
    PosixFallocateFailed {
        error: i32,
    },
    FtruncateFailed {
        error: i32,
    },
    InvalidCompressionType,
    TryingToWriteToReadOnlyFile,
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
            OmFilesRsError::CannotCreateFile {
                filename,
                errno,
                error,
            } => {
                write!(
                    f,
                    "Cannot create file '{}': errno {}, error: {}",
                    filename, errno, error
                )
            }
            OmFilesRsError::CannotTruncateFile {
                filename,
                errno,
                error,
            } => {
                write!(
                    f,
                    "Cannot truncate file '{}': errno {}, error: {}",
                    filename, errno, error
                )
            }
            OmFilesRsError::CannotOpenFileErrno { errno, error } => {
                write!(f, "Cannot open file: errno {}, error: {}", errno, error)
            }
            OmFilesRsError::CannotMoveFile {
                from,
                to,
                errno,
                error,
            } => {
                write!(
                    f,
                    "Cannot move file from '{}' to '{}': errno {}, error: {}",
                    from, to, errno, error
                )
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
            OmFilesRsError::NotAOmFile => {
                write!(f, "Not an Om file")
            }
            OmFilesRsError::InvalidHeaderLength => {
                write!(f, "Invalid header length")
            }
            OmFilesRsError::FileExistsAlready { filename } => {
                write!(f, "File '{}' already exists", filename)
            }
            OmFilesRsError::PosixFallocateFailed { error } => {
                write!(f, "Posix fallocate failed: error {}", error)
            }
            OmFilesRsError::FtruncateFailed { error } => {
                write!(f, "Ftruncate failed: error {}", error)
            }
            OmFilesRsError::InvalidCompressionType => {
                write!(f, "Invalid compression type")
            }
            OmFilesRsError::TryingToWriteToReadOnlyFile => {
                write!(f, "Trying to write to read-only file")
            }
        }
    }
}

impl std::error::Error for OmFilesRsError {}
