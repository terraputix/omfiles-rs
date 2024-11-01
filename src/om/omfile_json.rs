use omfileformatc_rs::{OmCompression_t, OmDataType_t};
use serde::{Deserialize, Serialize};

use crate::{compression::CompressionType, data_types::DataType};

/// The entry level JSON structure to decode all meta data inside an OpenMeteo file
/// Should contain an attribute `variable` with a list of variables in this file
#[derive(Serialize, Deserialize)]
pub struct OmFileJSON {
    /// A list of variables inside this file
    pub variables: Vec<OmFileJSONVariable>,

    pub some_attributes: Option<String>,
}

/// Represent a variable inside an OpenMeteo file.
/// A variable can have arbitrary attributes, but the following are required for decoding:
/// `dimensions` and `chunks` to describe the shape of data
/// `compression` and `scalefactor` define how data is compressed
/// `lutOffset` and `lutChunkSize` are required to locate data inside the file
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OmFileJSONVariable {
    pub name: Option<String>,

    /// The dimensions of the file
    pub dimensions: Vec<usize>,

    /// How the dimensions are chunked
    pub chunks: Vec<usize>,

    pub dimension_names: Option<Vec<String>>,

    /// The scalefactor that is applied to convert floating point values to integers
    pub scalefactor: f32,

    pub add_offset: f32,

    /// Type of compression and coding. E.g. delta, zigzag coding is then implemented in different compression routines
    pub compression: CompressionType,

    /// Data type like float, int32, uint64
    pub data_type: DataType,

    /// The offset position of the beginning of the look up table LUT. The LUT contains then data positions for each chunk
    pub lut_offset: usize,

    /// The total size of the compressed LUT.
    pub lut_size: usize,
}
