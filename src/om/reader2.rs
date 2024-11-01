use omfileformatc_rs::{OmDataType_t_DATA_TYPE_FLOAT, OmDecoder_readBufferSize, OmError_string};
use omfileformatc_rs::{OmDecoder_init, OmError_t_ERROR_OK};
use std::ops::Range;

use crate::compression::CompressionType;
use crate::data_types::{DataType, OmFileDataType};
use crate::om::decoder::create_decoder;

use super::backends::OmFileReaderBackend;
use super::errors::OmFilesRsError;
use super::header::OmHeader;
use super::omfile_json::{OmFileJSON, OmFileJSONVariable};

pub struct OmFileReader2<Backend: OmFileReaderBackend> {
    pub backend: Backend,
    pub json: OmFileJSON,
    lut_chunk_element_count: usize,
}

impl<Backend: OmFileReaderBackend> OmFileReader2<Backend> {
    pub fn new(backend: Backend, json: OmFileJSON, lut_chunk_element_count: usize) -> Self {
        Self {
            backend,
            json,
            lut_chunk_element_count,
        }
    }
}

// OmFileReader<Backend> {
//     pub fn new(backend: Backend) -> Result<Self, OmFilesRsError> {
//         // Fetch header
//         backend.pre_read(0, OmHeader::LENGTH)?;
//         let bytes = backend.get_bytes(0, OmHeader::LENGTH)?;
//         let header = OmHeader::from_bytes(bytes)?;

//         let dimensions = Dimensions::new(header.dim0, header.dim1, header.chunk0, header.chunk1);

//         Ok(Self {
//             backend,
//             dimensions: dimensions,
//             scalefactor: header.scalefactor,
//             compression: if header.version == 1 {
//                 CompressionType::P4nzdec256
//             } else {
//                 CompressionType::try_from(header.compression)?
//             },
//         })
//     }

impl<Backend: OmFileReaderBackend> OmFileReader2<Backend> {
    pub fn open_file(
        backend: Backend,
        lut_chunk_element_count: usize,
    ) -> Result<Self, OmFilesRsError> {
        let header_bytes = backend.get_bytes(0, 3)?;

        if header_bytes[0] != OmHeader::MAGIC_NUMBER1 || header_bytes[1] != OmHeader::MAGIC_NUMBER2
        {
            return Err(OmFilesRsError::NotAOmFile);
        }

        // handle versions 1 and 2
        let version = header_bytes[2];
        if version == 1 || version == 2 {
            // backend.pre_read(0, OmHeader::LENGTH)?;
            let header_bytes = backend.get_bytes(0, OmHeader::LENGTH)?;
            let header = OmHeader::from_bytes(&header_bytes)?;
            let variable = OmFileJSONVariable {
                name: Some("data".to_string()),
                dimensions: vec![header.dim0, header.dim1],
                chunks: vec![header.chunk0, header.chunk1],
                dimension_names: None,
                scalefactor: header.scalefactor,
                add_offset: 0.0,
                compression: header.compression, // TODO: avoid type cast
                data_type: DataType::Float,
                lut_offset: OmHeader::LENGTH,
                lut_size: 8,
            };
            let json = OmFileJSON {
                variables: vec![variable],
                some_attributes: None,
            };
            return Ok(OmFileReader2 {
                backend,
                json,
                lut_chunk_element_count: 1,
            });
        }

        if version != 3 {
            return Err(OmFilesRsError::UnknownVersion(version));
        }

        let file_size = backend.count();
        backend.pre_read(file_size - 8, 8)?;
        let json_length_bytes = backend.get_bytes(file_size - 8, 8)?;
        let json_length = u64::from_le_bytes(
            json_length_bytes
                .try_into()
                .expect("Slice with incorrect length"),
        ) as usize;

        // backend.pre_read(file_size - 8 - json_length, json_length)?;
        let json_data = backend.get_bytes(file_size - 8 - json_length, json_length)?;

        let json: OmFileJSON = serde_json::from_slice(json_data).expect("Failed to parse JSON");
        Ok(OmFileReader2 {
            backend,
            json,
            lut_chunk_element_count,
        })
    }

    /// Get all variables combined with a reference to the file handle to keep it open.
    pub fn get_variables(&self) -> Vec<OmFileVariableReader<Backend>> {
        self.json
            .variables
            .iter()
            .map(|variable| OmFileVariableReader {
                backend: self.backend,
                variable: variable.clone(),
                lut_chunk_element_count: self.lut_chunk_element_count,
            })
            .collect()
    }

    pub fn read(
        &self,
        into: &mut [f32],
        dim_read: &[Range<usize>],
        into_cube_offset: &[usize],
        into_cube_dimension: &[usize],
        io_size_max: usize,
        io_size_merge: usize,
    ) -> Result<(), OmFilesRsError> {
        let v = &self.json.variables[0];
        println!("v: {:?}", v);
        let n_dimensions = v.dimensions.len();
        assert_eq!(dim_read.len(), n_dimensions);
        assert_eq!(into_cube_offset.len(), n_dimensions);
        assert_eq!(into_cube_dimension.len(), n_dimensions);

        let read_offset: Vec<usize> = dim_read.iter().map(|r| r.start).collect();
        let read_count: Vec<usize> = dim_read.iter().map(|r| r.end - r.start).collect();

        let mut decoder = create_decoder();
        unsafe {
            OmDecoder_init(
                &mut decoder,
                v.scalefactor,
                v.add_offset,
                v.compression.to_c(),
                v.data_type.to_c(),
                v.dimensions.len(),
                v.dimensions.as_ptr(),
                v.chunks.as_ptr(),
                read_offset.as_ptr(),
                read_count.as_ptr(),
                into_cube_offset.as_ptr(),
                into_cube_dimension.as_ptr(),
                v.lut_size,
                self.lut_chunk_element_count,
                v.lut_offset,
                io_size_merge,
                io_size_max,
            );
        }

        let chunk_buffer_size = unsafe { OmDecoder_readBufferSize(&decoder) };
        let mut chunk_buffer = vec![0u8; chunk_buffer_size as usize];
        self.backend.decode(&decoder, into, &mut chunk_buffer)?;

        Ok(())
    }

    pub fn read_simple(
        &self,
        dim_read: &[Range<usize>],
        io_size_max: usize,
        io_size_merge: usize,
    ) -> Result<Vec<f32>, OmFilesRsError> {
        let out_dims: Vec<usize> = dim_read.iter().map(|r| r.end - r.start).collect();
        let n = out_dims.iter().product::<usize>() as usize;
        let mut out = vec![f32::NAN; n];
        self.read(
            &mut out,
            dim_read,
            &vec![0; dim_read.len()],
            &out_dims,
            io_size_max,
            io_size_merge,
        )?;
        Ok(out)
    }
}

/// Reader for a single variable, holding a reference to the file handle.
pub struct OmFileVariableReader<Backend: OmFileReaderBackend> {
    backend: Backend,
    variable: OmFileJSONVariable,
    lut_chunk_element_count: usize,
}

impl<Backend: OmFileReaderBackend> OmFileVariableReader<Backend> {
    /// Read the variable as `f32`.
    pub fn read(
        &self,
        dim_read: &[Range<usize>],
        io_size_max: usize,
        io_size_merge: usize,
    ) -> Vec<f32> {
        let out_dims: Vec<usize> = dim_read.iter().map(|r| r.end - r.start).collect();
        let n: usize = out_dims.iter().product();
        let mut out = vec![f32::NAN; n];

        self.read_into(
            &mut out,
            dim_read,
            &vec![0; dim_read.len()],
            &out_dims,
            io_size_max,
            io_size_merge,
        );
        out
    }

    /// Read a variable from an OM file into the provided buffer.
    pub fn read_into<OmType: OmFileDataType>(
        &self,
        into: &mut [OmType],
        dim_read: &[Range<usize>],
        into_cube_offset: &[usize],
        into_cube_dimension: &[usize],
        io_size_max: usize,
        io_size_merge: usize,
    ) {
        let n_dimensions = self.variable.dimensions.len();
        assert_eq!(OmType::DATA_TYPE, self.variable.data_type);
        assert_eq!(dim_read.len(), n_dimensions);
        assert_eq!(into_cube_offset.len(), n_dimensions);
        assert_eq!(into_cube_dimension.len(), n_dimensions);

        let read_offset: Vec<usize> = dim_read.iter().map(|r| r.start).collect();
        let read_count: Vec<usize> = dim_read.iter().map(|r| (r.end - r.start)).collect();

        let mut decoder = create_decoder();
        let error = unsafe {
            OmDecoder_init(
                &mut decoder,
                self.variable.scalefactor,
                self.variable.add_offset,
                self.variable.compression.to_c(),
                self.variable.data_type.to_c(),
                n_dimensions,
                self.variable.dimensions.as_ptr(),
                self.variable.chunks.as_ptr(),
                read_offset.as_ptr(),
                read_count.as_ptr(),
                into_cube_offset.as_ptr(),
                into_cube_dimension.as_ptr(),
                self.variable.lut_size,
                self.lut_chunk_element_count,
                self.variable.lut_offset,
                io_size_merge,
                io_size_max,
            )
        };
        if error != OmError_t_ERROR_OK {
            panic!("OmDecoder: {}", unsafe {
                std::ffi::CStr::from_ptr(OmError_string(error))
                    .to_string_lossy()
                    .into_owned()
            });
        }

        let chunk_buffer_size = unsafe { OmDecoder_readBufferSize(&mut decoder) } as usize;
        let mut chunk_buffer = vec![0u8; chunk_buffer_size];
        self.backend
            .decode(&mut decoder, into, chunk_buffer.as_mut_slice());
    }
}
