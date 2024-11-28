use omfileformatc_rs::{
    om_decoder_init, om_decoder_read_buffer_size, om_error_string, om_trailer_read,
    om_trailer_size, om_variable_get_add_offset, om_variable_get_child, om_variable_get_chunks,
    om_variable_get_compression, om_variable_get_dimensions, om_variable_get_name,
    om_variable_get_number_of_children, om_variable_get_scalar, om_variable_get_scale_factor,
    om_variable_get_type, OmDecoder_t, OmError_t_ERROR_OK, OmHeaderType_t_OM_HEADER_LEGACY,
    OmHeaderType_t_OM_HEADER_READ_TRAILER,
};
// use omfileformatc_rs::{OmDecoder_readBufferSize, OmError_string};
use std::fs::File;
use std::ops::Range;
use std::os::raw::c_void;
use std::ptr::NonNull;

use crate::compression::CompressionType;
use crate::data_types::{DataType, OmFileArrayDataType};
use crate::om::c_defaults::create_decoder;

use super::backends::OmFileReaderBackend;
use super::errors::OmFilesRsError;
use super::header::{self, OmHeader};
use super::mmapfile::{MmapFile, Mode};
use crate::data_types::OmFileScalarDataType;
use omfileformatc_rs::{
    om_header_size, om_header_type, om_variable_init, OmHeaderType_t_OM_HEADER_INVALID,
    OmVariable_t,
};

pub struct OmFileReader2<Backend: OmFileReaderBackend> {
    /// Points to the underlying memory. Needs to remain in scope to keep memory accessible
    pub backend: Backend,
    pub variable: *const OmVariable_t,
    /// Number of elements in index LUT chunk. Assumed to be 256 in production files. Only used for testing!
    pub lut_chunk_element_count: u64,
}

impl<Backend: OmFileReaderBackend> OmFileReader2<Backend> {
    pub fn new(backend: Backend, lut_chunk_element_count: u64) -> Self {
        let header_size = unsafe { om_header_size() };
        let header_data = backend
            .get_bytes(0, header_size)
            .expect("Failed to read header");

        let header_type = unsafe { om_header_type(header_data.as_ptr() as *const c_void) };

        let variable = unsafe {
            match header_type {
                OmHeaderType_t_OM_HEADER_LEGACY => {
                    om_variable_init(header_data.as_ptr() as *const c_void)
                }
                OmHeaderType_t_OM_HEADER_READ_TRAILER => {
                    let file_size = backend.count();
                    let trailer_size = om_trailer_size();
                    let trailer_data = backend
                        .get_bytes(file_size - trailer_size, trailer_size)
                        .expect("Failed to read trailer");
                    let position = om_trailer_read(trailer_data.as_ptr() as *const c_void);

                    if position.size == 0 {
                        panic!("Not a valid OM file");
                    }

                    let data_variable = backend
                        .get_bytes(position.offset as usize, position.size as usize)
                        .expect("Failed to read data variable");
                    om_variable_init(data_variable.as_ptr() as *const c_void)
                }
                OmHeaderType_t_OM_HEADER_INVALID => {
                    panic!("Not a valid OM file");
                }
                _ => {
                    panic!("Unknown header type");
                }
            }
        };
        Self {
            backend,
            variable,
            lut_chunk_element_count,
        }
    }

    pub fn data_type(&self) -> DataType {
        unsafe {
            DataType::try_from(om_variable_get_type(self.variable) as u8)
                .expect("Invalid data type")
        }
    }

    pub fn compression(&self) -> CompressionType {
        unsafe {
            CompressionType::try_from(om_variable_get_compression(self.variable) as u8)
                .expect("Invalid compression type")
        }
    }

    pub fn scale_factor(&self) -> f32 {
        unsafe { om_variable_get_scale_factor(self.variable) }
    }

    pub fn add_offset(&self) -> f32 {
        unsafe { om_variable_get_add_offset(self.variable) }
    }

    pub fn get_dimensions(&self) -> &[u64] {
        unsafe {
            let dims = om_variable_get_dimensions(self.variable);
            std::slice::from_raw_parts(dims.values, dims.count as usize)
        }
    }

    pub fn get_chunk_dimensions(&self) -> &[u64] {
        unsafe {
            let chunks = om_variable_get_chunks(self.variable);
            std::slice::from_raw_parts(chunks.values, chunks.count as usize)
        }
    }

    pub fn get_name(&self) -> Option<String> {
        unsafe {
            let name = om_variable_get_name(self.variable);
            if name.size == 0 {
                return None;
            }
            let bytes = std::slice::from_raw_parts(name.value as *const u8, name.size as usize);
            String::from_utf8(bytes.to_vec()).ok()
        }
    }

    pub fn number_of_children(&self) -> u32 {
        unsafe { om_variable_get_number_of_children(self.variable) }
    }

    pub fn get_child(&self, index: i32) -> Option<Self> {
        unimplemented!("Need to use RC for backend");
        // unsafe {
        //     let child = om_variable_get_child(self.variable, index);
        //     if child.size == 0 {
        //         return None;
        //     }
        //     let data_child = self
        //         .backend
        //         .get_bytes(child.offset as usize, child.size as usize)
        //         .expect("Failed to read child data");
        //     let child_variable = om_variable_init(data_child.as_ptr() as *const c_void);
        //     Some(Self {
        //         backend: self.backend.clone(),
        //         variable: child_variable,
        //         lut_chunk_element_count: self.lut_chunk_element_count,
        //     })
        // }
    }

    pub fn read_scalar<T: OmFileScalarDataType>(&self) -> Option<T> {
        if T::DATA_TYPE_SCALAR != self.data_type() {
            return None;
        }
        let mut value = T::default();
        unsafe {
            if om_variable_get_scalar(self.variable, &mut value as *mut T as *mut c_void)
                != OmError_t_ERROR_OK
            {
                return None;
            }
        }
        Some(value)
    }
}

impl OmFileReader2<MmapFile> {
    /// Convenience initializer to create an `OmFileReader` from a file path.
    pub fn from_file(file: &str) -> Result<Self, OmFilesRsError> {
        let file_handle = File::open(file).map_err(|e| OmFilesRsError::CannotOpenFile {
            filename: file.to_string(),
            errno: e.raw_os_error().unwrap_or(0),
            error: e.to_string(),
        })?;
        Self::from_file_handle(file_handle)
    }

    /// Convenience initializer to create an `OmFileReader` from an existing `FileHandle`.
    pub fn from_file_handle(file_handle: File) -> Result<Self, OmFilesRsError> {
        // TODO: Error handling
        let mmap = MmapFile::new(file_handle, Mode::ReadOnly).unwrap();
        Ok(Self::new(mmap, 256)) // FIXME
    }

    /// Check if the file was deleted on the file system.
    /// Linux keeps the file alive as long as some processes have it open.
    pub fn was_deleted(&self) -> bool {
        self.backend.was_deleted()
    }
}

impl<Backend: OmFileReaderBackend> OmFileReader2<Backend> {
    /// Read a variable as an array of a dynamic data type.
    pub fn read_into<T: OmFileArrayDataType>(
        &self,
        into: &mut [T],
        dim_read: &[Range<u64>],
        into_cube_offset: &[u64],
        into_cube_dimension: &[u64],
        io_size_max: Option<u64>,
        io_size_merge: Option<u64>,
    ) -> Result<(), OmFilesRsError> {
        let io_size_max = io_size_max.unwrap_or(65536);
        let io_size_merge = io_size_merge.unwrap_or(512);

        // Verify data type
        if T::DATA_TYPE_ARRAY != self.data_type() {
            return Err(OmFilesRsError::InvalidDataType);
        }

        let n_dimensions = dim_read.len();

        // Validate dimension counts
        assert_eq!(into_cube_offset.len(), n_dimensions);
        assert_eq!(into_cube_dimension.len(), n_dimensions);

        // Prepare read parameters
        let read_offset: Vec<u64> = dim_read.iter().map(|r| r.start).collect();
        let read_count: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();

        // Initialize decoder
        let mut decoder = unsafe { std::mem::zeroed::<OmDecoder_t>() };
        let error = unsafe {
            om_decoder_init(
                &mut decoder,
                self.variable,
                n_dimensions as u64,
                read_offset.as_ptr(),
                read_count.as_ptr(),
                into_cube_offset.as_ptr(),
                into_cube_dimension.as_ptr(),
                self.lut_chunk_element_count as u64,
                io_size_merge,
                io_size_max,
            )
        };

        if error != OmError_t_ERROR_OK {
            let error_str = unsafe {
                let ptr = om_error_string(error);
                std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
            };
            return Err(OmFilesRsError::DecoderError(error_str));
        }

        // Allocate chunk buffer
        let chunk_buffer_size = unsafe { om_decoder_read_buffer_size(&decoder) };
        let mut chunk_buffer = Vec::<u8>::with_capacity(chunk_buffer_size as usize);

        // Perform decoding
        self.backend
            .decode(&mut decoder, into, chunk_buffer.as_mut_slice())?;

        Ok(())
    }

    pub fn read_simple(
        &self,
        dim_read: &[Range<u64>],
        io_size_max: Option<u64>,
        io_size_merge: Option<u64>,
    ) -> Result<Vec<f32>, OmFilesRsError> {
        let out_dims: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();
        let n = out_dims.iter().product::<u64>() as usize;
        let mut out = vec![f32::NAN; n];

        self.read_into::<f32>(
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

// /// Reader for a single variable, holding a reference to the file handle.
// pub struct OmFileVariableReader<'a, Backend: OmFileReaderBackend> {
//     backend: &'a Backend,
//     variable: OmFileJSONVariable,
//     lut_chunk_element_count: usize,
// }

// impl<'a, Backend: OmFileReaderBackend> OmFileVariableReader<'a, Backend> {
//     /// Read the variable as `f32`.
//     pub fn read(
//         &self,
//         dim_read: &[Range<usize>],
//         io_size_max: usize,
//         io_size_merge: usize,
//     ) -> Vec<f32> {
//         let out_dims: Vec<usize> = dim_read.iter().map(|r| r.end - r.start).collect();
//         let n: usize = out_dims.iter().product();
//         let mut out = vec![f32::NAN; n];

//         self.read_into(
//             &mut out,
//             dim_read,
//             &vec![0; dim_read.len()],
//             &out_dims,
//             io_size_max,
//             io_size_merge,
//         );
//         out
//     }

//     /// Read a variable from an OM file into the provided buffer.
//     pub fn read_into<OmType: OmFileDataType>(
//         &self,
//         into: &mut [OmType],
//         dim_read: &[Range<usize>],
//         into_cube_offset: &[usize],
//         into_cube_dimension: &[usize],
//         io_size_max: usize,
//         io_size_merge: usize,
//     ) {
//         let n_dimensions = self.variable.dimensions.len();
//         assert_eq!(OmType::DATA_TYPE, self.variable.data_type);
//         assert_eq!(dim_read.len(), n_dimensions);
//         assert_eq!(into_cube_offset.len(), n_dimensions);
//         assert_eq!(into_cube_dimension.len(), n_dimensions);

//         let read_offset: Vec<usize> = dim_read.iter().map(|r| r.start).collect();
//         let read_count: Vec<usize> = dim_read.iter().map(|r| (r.end - r.start)).collect();

//         let mut decoder = create_decoder();
//         let error = unsafe {
//             OmDecoder_init(
//                 &mut decoder,
//                 self.variable.scalefactor,
//                 self.variable.add_offset,
//                 self.variable.compression.to_c(),
//                 self.variable.data_type.to_c(),
//                 n_dimensions,
//                 self.variable.dimensions.as_ptr(),
//                 self.variable.chunks.as_ptr(),
//                 read_offset.as_ptr(),
//                 read_count.as_ptr(),
//                 into_cube_offset.as_ptr(),
//                 into_cube_dimension.as_ptr(),
//                 self.variable.lut_size,
//                 self.lut_chunk_element_count,
//                 self.variable.lut_offset,
//                 io_size_merge,
//                 io_size_max,
//             )
//         };
//         if error != OmError_t_ERROR_OK {
//             panic!("OmDecoder: {}", unsafe {
//                 std::ffi::CStr::from_ptr(om_error_string(error))
//                     .to_string_lossy()
//                     .into_owned()
//             });
//         }

//         let chunk_buffer_size = unsafe { om_decoder_read_buffer_size(&mut decoder) } as usize;
//         let mut chunk_buffer = vec![0u8; chunk_buffer_size];
//         self.backend
//             .decode(&mut decoder, into, chunk_buffer.as_mut_slice())
//             .expect("Unexpected error in OmDecoder");
//     }
// }
