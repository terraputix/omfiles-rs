#![allow(non_snake_case)]
use crate::backend::backends::{OmFileReaderBackend, OmFileReaderBackendAsync};
use crate::backend::mmapfile::{MmapFile, Mode};
use crate::core::compression::CompressionType;
use crate::core::data_types::{DataType, OmFileArrayDataType, OmFileScalarDataType};
use crate::core::wrapped_decoder::WrappedDecoder;
use crate::errors::OmFilesRsError;
use crate::io::variable::OmVariablePtr;
use crate::io::writer::OmOffsetSize;
use ndarray::ArrayD;
use num_traits::Zero;
use om_file_format_sys::{
    om_header_size, om_header_type, om_trailer_read, om_trailer_size, om_variable_get_add_offset,
    om_variable_get_children, om_variable_get_children_count, om_variable_get_chunks,
    om_variable_get_compression, om_variable_get_dimensions, om_variable_get_name,
    om_variable_get_scalar, om_variable_get_scale_factor, om_variable_get_type, om_variable_init,
    OmError_t, OmHeaderType_t,
};
use std::collections::HashMap;
use std::fs::File;
use std::ops::Range;
use std::os::raw::c_void;
use std::sync::Arc;

#[derive(Clone)]
pub struct OmFileReader<Backend> {
    offset_size: Option<OmOffsetSize>,
    /// The backend that provides data via the get_bytes method
    pub backend: Arc<Backend>,
    /// Holds the data where the meta information of the variable is stored, is not supposed to go out of scope
    /// Here the LUT and additional attributes of the variable need to be stored.
    pub variable_data: Vec<u8>,
    /// Opaque pointer to the variable defined by header/trailer
    pub(crate) variable: OmVariablePtr,
}

impl<Backend> OmFileReader<Backend> {
    pub fn data_type(&self) -> DataType {
        unsafe {
            DataType::try_from(om_variable_get_type(*self.variable) as u8)
                .expect("Invalid data type")
        }
    }

    pub fn compression(&self) -> CompressionType {
        unsafe {
            CompressionType::try_from(om_variable_get_compression(*self.variable) as u8)
                .expect("Invalid compression type")
        }
    }

    pub fn scale_factor(&self) -> f32 {
        unsafe { om_variable_get_scale_factor(*self.variable) }
    }

    pub fn add_offset(&self) -> f32 {
        unsafe { om_variable_get_add_offset(*self.variable) }
    }

    pub fn get_dimensions(&self) -> &[u64] {
        unsafe {
            let dims = om_variable_get_dimensions(*self.variable);
            std::slice::from_raw_parts(dims.values, dims.count as usize)
        }
    }

    pub fn get_chunk_dimensions(&self) -> &[u64] {
        unsafe {
            let chunks = om_variable_get_chunks(*self.variable);
            std::slice::from_raw_parts(chunks.values, chunks.count as usize)
        }
    }

    pub fn get_name(&self) -> Option<String> {
        unsafe {
            let name = om_variable_get_name(*self.variable);
            if name.size == 0 {
                return None;
            }
            let bytes = std::slice::from_raw_parts(name.value as *const u8, name.size as usize);
            String::from_utf8(bytes.to_vec()).ok()
        }
    }

    pub fn number_of_children(&self) -> u32 {
        unsafe { om_variable_get_children_count(*self.variable) }
    }

    pub fn read_scalar<T: OmFileScalarDataType>(&self) -> Option<T> {
        if T::DATA_TYPE_SCALAR != self.data_type() {
            return None;
        }

        let mut ptr: *mut std::os::raw::c_void = std::ptr::null_mut();
        let mut size: u64 = 0;

        let error = unsafe { om_variable_get_scalar(*self.variable, &mut ptr, &mut size) };

        if error != OmError_t::ERROR_OK || ptr.is_null() {
            return None;
        }

        // Safety: ptr points to a valid memory region of 'size' bytes
        // that contains data of the expected type
        let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, size as usize) };

        Some(T::from_raw_bytes(bytes))
    }

    /// Shared initialization from variable data
    fn init_from_variable_data(
        backend: Arc<Backend>,
        variable_data: Vec<u8>,
        offset_size: Option<OmOffsetSize>,
    ) -> Result<Self, OmFilesRsError> {
        let variable_ptr = unsafe { om_variable_init(variable_data.as_ptr() as *const c_void) };
        Ok(Self {
            offset_size,
            backend,
            variable_data,
            variable: OmVariablePtr(variable_ptr),
        })
    }

    /// Helper to process trailer data
    unsafe fn process_trailer(trailer_data: &[u8]) -> Result<(u64, u64), OmFilesRsError> {
        let mut offset = 0u64;
        let mut size = 0u64;
        if !om_trailer_read(
            trailer_data.as_ptr() as *const c_void,
            &mut offset,
            &mut size,
        ) {
            return Err(OmFilesRsError::NotAnOmFile);
        }

        Ok((offset, size))
    }

    pub(crate) fn prepare_read_parameters<T: OmFileArrayDataType>(
        &self,
        dim_read: &[Range<u64>],
        into_cube_offset: &[u64],
        into_cube_dimension: &[u64],
        io_size_max: Option<u64>,
        io_size_merge: Option<u64>,
    ) -> Result<WrappedDecoder, OmFilesRsError> {
        let io_size_max = io_size_max.unwrap_or(65536);
        let io_size_merge = io_size_merge.unwrap_or(512);

        // Verify data type
        if T::DATA_TYPE_ARRAY != self.data_type() {
            return Err(OmFilesRsError::InvalidDataType);
        }

        let n_dimensions_read = dim_read.len();
        let n_dims = self.get_dimensions().len();

        // Validate dimension counts
        if n_dims != n_dimensions_read
            || n_dimensions_read != into_cube_offset.len()
            || n_dimensions_read != into_cube_dimension.len()
        {
            return Err(OmFilesRsError::MismatchingCubeDimensionLength);
        }

        // Prepare read parameters
        let read_offset: Vec<u64> = dim_read.iter().map(|r| r.start).collect();
        let read_count: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();

        // Initialize decoder
        let decoder = WrappedDecoder::new(
            self.variable,
            n_dimensions_read as u64,
            read_offset,
            read_count,
            into_cube_offset,
            into_cube_dimension,
            io_size_merge,
            io_size_max,
        )?;

        Ok(decoder)
    }
}

impl<Backend: OmFileReaderBackend> OmFileReader<Backend> {
    pub fn new(backend: Arc<Backend>) -> Result<Self, OmFilesRsError> {
        let header_size = unsafe { om_header_size() };
        if backend.count() < header_size {
            return Err(OmFilesRsError::FileTooSmall);
        }
        let header_data = backend.get_bytes_with_fallback(0, header_size as u64)?;
        let header_type = unsafe { om_header_type(header_data.as_ptr() as *const c_void) };

        let (variable_data, offset_size) = {
            match header_type {
                OmHeaderType_t::OM_HEADER_LEGACY => (header_data.into_owned(), None),
                OmHeaderType_t::OM_HEADER_READ_TRAILER => {
                    let file_size = backend.count();
                    let trailer_size = unsafe { om_trailer_size() };
                    let trailer_data = backend.get_bytes_with_fallback(
                        (file_size - trailer_size) as u64,
                        trailer_size as u64,
                    )?;

                    let (offset, size) = unsafe { Self::process_trailer(&trailer_data) }?;

                    let offset_size = OmOffsetSize::new(offset, size);
                    let variable_data = backend.get_bytes_with_fallback(offset, size)?.into_owned();
                    (variable_data, Some(offset_size))
                }
                OmHeaderType_t::OM_HEADER_INVALID => {
                    return Err(OmFilesRsError::NotAnOmFile);
                }
            }
        };

        Self::init_from_variable_data(backend, variable_data, offset_size)
    }

    /// Returns a HashMap mapping variable names to their offset and size
    /// This function needs to traverse the entire variable tree, therefore
    /// it is best to make sure that variable metadata is close to each other
    /// at the end of the file (before the trailer). The caller could then
    /// make sure that this part of the file is loaded/cached in memory
    pub fn get_flat_variable_metadata(&self) -> HashMap<String, OmOffsetSize> {
        let mut result = HashMap::new();
        self.collect_variable_metadata(Vec::new(), &mut result);
        result
    }

    /// Helper function that recursively collects variable metadata
    fn collect_variable_metadata(
        &self,
        mut current_path: Vec<String>,
        result: &mut HashMap<String, OmOffsetSize>,
    ) {
        // Add current variable's metadata if it has a name and offset_size
        // TODO: This requires for names to be unique
        if let Some(name) = self.get_name() {
            if let Some(offset_size) = &self.offset_size {
                current_path.push(name.to_string());
                // Create hierarchical key
                let path_str = current_path
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join("/");

                result.insert(path_str, offset_size.clone());
            }
        }

        // Process children
        let num_children = self.number_of_children();
        for i in 0..num_children {
            let child_path = current_path.clone();
            if let Some(child) = self.get_child(i) {
                child.collect_variable_metadata(child_path, result);
            }
        }
    }

    pub fn get_child(&self, index: u32) -> Option<Self> {
        let mut offset = 0u64;
        let mut size = 0u64;
        if !unsafe { om_variable_get_children(*self.variable, index, 1, &mut offset, &mut size) } {
            return None;
        }

        let offset_size = OmOffsetSize::new(offset, size);
        let child = self
            .init_child_from_offset_size(offset_size)
            .expect("Failed to init child");
        Some(child)
    }

    pub fn init_child_from_offset_size(
        &self,
        offset_size: OmOffsetSize,
    ) -> Result<Self, OmFilesRsError> {
        let child_variable = self
            .backend
            .get_bytes_with_fallback(offset_size.offset, offset_size.size)?
            .into_owned();

        let child_variable_ptr =
            unsafe { om_variable_init(child_variable.as_ptr() as *const c_void) };

        Ok(Self {
            offset_size: Some(offset_size),
            backend: self.backend.clone(),
            variable_data: child_variable,
            variable: OmVariablePtr(child_variable_ptr),
        })
    }

    /// Read a variable as an array of a dynamic data type.
    pub fn read_into<T: OmFileArrayDataType>(
        &self,
        into: &mut ArrayD<T>,
        dim_read: &[Range<u64>],
        into_cube_offset: &[u64],
        into_cube_dimension: &[u64],
        io_size_max: Option<u64>,
        io_size_merge: Option<u64>,
    ) -> Result<(), OmFilesRsError> {
        let decoder = self.prepare_read_parameters::<T>(
            dim_read,
            into_cube_offset,
            into_cube_dimension,
            io_size_max,
            io_size_merge,
        )?;

        // Allocate chunk buffer
        let mut chunk_buffer = Vec::<u8>::with_capacity(decoder.buffer_size() as usize);

        // Perform decoding
        self.backend
            .decode(&decoder.decoder, into, chunk_buffer.as_mut_slice())?;

        Ok(())
    }

    pub fn read<T: OmFileArrayDataType + Clone + Zero>(
        &self,
        dim_read: &[Range<u64>],
        io_size_max: Option<u64>,
        io_size_merge: Option<u64>,
    ) -> Result<ArrayD<T>, OmFilesRsError> {
        let out_dims: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();
        let out_dims_usize = out_dims.iter().map(|&x| x as usize).collect::<Vec<_>>();

        let mut out = ArrayD::<T>::zeros(out_dims_usize);

        self.read_into::<T>(
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

impl OmFileReader<MmapFile> {
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
        Self::new(Arc::new(mmap)) // FIXME
    }

    /// Check if the file was deleted on the file system.
    /// Linux keeps the file alive as long as some processes have it open.
    pub fn was_deleted(&self) -> bool {
        self.backend.was_deleted()
    }
}

impl<Backend: OmFileReaderBackendAsync> OmFileReader<Backend> {
    pub async fn async_new(backend: Arc<Backend>) -> Result<Self, OmFilesRsError> {
        let header_size = unsafe { om_header_size() };
        if backend.count_async() < header_size {
            return Err(OmFilesRsError::FileTooSmall);
        }
        let header_data = backend.get_bytes_async(0, header_size as u64).await?;
        let header_type = unsafe { om_header_type(header_data.as_ptr() as *const c_void) };

        let (variable_data, offset_size) = {
            match header_type {
                OmHeaderType_t::OM_HEADER_LEGACY => (header_data, None),
                OmHeaderType_t::OM_HEADER_READ_TRAILER => unsafe {
                    let file_size = backend.count_async();
                    let trailer_size = om_trailer_size();
                    let trailer_data = backend
                        .get_bytes_async((file_size - trailer_size) as u64, trailer_size as u64)
                        .await?;

                    let (offset, size) = Self::process_trailer(&trailer_data)?;
                    let offset_size = OmOffsetSize::new(offset, size);

                    let variable_data = backend.get_bytes_async(offset, size).await?;
                    (variable_data, Some(offset_size))
                },
                OmHeaderType_t::OM_HEADER_INVALID => {
                    return Err(OmFilesRsError::NotAnOmFile);
                }
            }
        };

        Self::init_from_variable_data(backend, variable_data, offset_size)
    }
}
