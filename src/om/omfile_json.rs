use omfileformatc_rs::{om_compression_t, om_datatype_t};
use serde::{Deserialize, Serialize};

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
#[derive(Serialize, Deserialize, Debug)]
pub struct OmFileJSONVariable {
    pub name: Option<String>,

    /// The dimensions of the file
    pub dimensions: Vec<u64>,

    /// How the dimensions are chunked
    pub chunks: Vec<u64>,

    pub dimension_names: Option<Vec<String>>,

    /// The scalefactor that is applied to convert floating point values to integers
    pub scalefactor: f32,

    /// Type of compression and coding. E.g. delta, zigzag coding is then implemented in different compression routines
    pub compression: om_compression_t,

    /// Data type like float, int32, uint64
    pub data_type: om_datatype_t,

    /// The offset position of the beginning of the look up table LUT. The LUT contains then data positions for each chunk
    pub lut_offset: u64,

    /// How long a chunk inside the LUT is after compression
    pub lut_chunk_size: u64,
}
