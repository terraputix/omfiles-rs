use omfileformatc_rs::om_decoder_init;
use omfileformatc_rs::{om_datatype_t_DATA_TYPE_FLOAT, om_decoder_read_buffer_size};
use std::ops::Range;

use crate::compression::CompressionType;
use crate::om::decoder::create_decoder;

use super::backends::OmFileReaderBackend;
use super::errors::OmFilesRsError;
use super::header::OmHeader;
use super::omfile_json::{OmFileJSON, OmFileJSONVariable};

pub struct OmFileReader2<Backend: OmFileReaderBackend> {
    backend: Backend,
    json: OmFileJSON,
    lut_chunk_element_count: u64,
}

impl<Backend: OmFileReaderBackend> OmFileReader2<Backend> {
    pub fn open_file(
        backend: Backend,
        lut_chunk_element_count: u64,
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
                dimensions: vec![header.dim0 as u64, header.dim1 as u64],
                chunks: vec![header.chunk0 as u64, header.chunk1 as u64],
                dimension_names: None,
                scalefactor: header.scalefactor,
                compression: header.compression as u32, // TODO: avoid type cast
                data_type: om_datatype_t_DATA_TYPE_FLOAT,
                lut_offset: OmHeader::LENGTH as u64,
                lut_chunk_size: 8,
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

    pub fn read(
        &self,
        into: &mut [f32],
        dim_read: &[Range<u64>],
        into_cube_offset: &[u64],
        into_cube_dimension: &[u64],
        io_size_max: u64,
        io_size_merge: u64,
    ) -> Result<(), OmFilesRsError> {
        let v = &self.json.variables[0];
        println!("v: {:?}", v);
        let n_dimensions = v.dimensions.len();
        assert_eq!(dim_read.len(), n_dimensions);
        assert_eq!(into_cube_offset.len(), n_dimensions);
        assert_eq!(into_cube_dimension.len(), n_dimensions);

        let read_offset: Vec<u64> = dim_read.iter().map(|r| r.start).collect();
        let read_count: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();

        let mut decoder = create_decoder();
        unsafe {
            om_decoder_init(
                &mut decoder,
                v.scalefactor,
                v.compression,
                v.data_type,
                v.dimensions.len() as u64,
                v.dimensions.as_ptr(),
                v.chunks.as_ptr(),
                read_offset.as_ptr(),
                read_count.as_ptr(),
                into_cube_offset.as_ptr(),
                into_cube_dimension.as_ptr(),
                v.lut_chunk_size,
                self.lut_chunk_element_count,
                v.lut_offset,
                io_size_merge,
                io_size_max,
            );
        }

        let chunk_buffer_size = unsafe { om_decoder_read_buffer_size(&decoder) };
        let mut chunk_buffer = vec![0u8; chunk_buffer_size as usize];
        self.backend.decode(&decoder, into, &mut chunk_buffer)?;

        Ok(())
    }

    pub fn read_simple(
        &self,
        dim_read: &[Range<u64>],
        io_size_max: u64,
        io_size_merge: u64,
    ) -> Result<Vec<f32>, OmFilesRsError> {
        let out_dims: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();
        let n = out_dims.iter().product::<u64>() as usize;
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
