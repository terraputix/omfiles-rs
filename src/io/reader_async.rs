#![allow(non_snake_case)]
use crate::backend::backends::OmFileReaderBackend;
use crate::core::c_defaults::{
    c_error_string, create_uninit_decoder, new_data_read, new_index_read,
};
use crate::core::data_types::OmFileArrayDataType;
use crate::errors::OmFilesRsError;
use crate::io::reader::{OmFileReader, OmVariablePtr};
use async_executor::Executor;
use async_lock::Semaphore;
use blocking::unblock;
use ndarray::ArrayD;
use num_traits::Zero;
use om_file_format_sys::{
    om_decoder_decode_chunks, om_decoder_init, om_decoder_next_data_read,
    om_decoder_next_index_read, om_decoder_read_buffer_size, OmDecoder_indexRead_t, OmDecoder_t,
    OmError_t, OmRange_t,
};
use std::cell::UnsafeCell;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::os::raw::c_void;
use std::sync::{Arc, OnceLock};

/// Executor for asynchronous tasks
/// TODO: this could potentially be moved to the reader level
static EXECUTOR: OnceLock<Executor<'static>> = OnceLock::new();
/// Get the executor for asynchronous tasks
fn get_executor() -> &'static Executor<'static> {
    EXECUTOR.get_or_init(|| Executor::new())
}

/// Maximum number of concurrent tasks for async operations
/// TODO: this could potentially be moved to the reader level
const MAX_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(8).unwrap();

impl<Backend: OmFileReaderBackend + Send + Sync + 'static> OmFileReader<Backend> {
    /// Read a variable asynchronously, using concurrent fetching and decoding
    pub async fn read_async<T: OmFileArrayDataType + Clone + Zero + Send + Sync + 'static>(
        &self,
        dim_read: &[Range<u64>],
        io_size_max: Option<u64>,
        io_size_merge: Option<u64>,
    ) -> Result<ArrayD<T>, OmFilesRsError> {
        let out_dims: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();
        let out_dims_usize = out_dims.iter().map(|&x| x as usize).collect::<Vec<_>>();

        let out = Arc::new(SharedArray::new(ArrayD::<T>::zeros(out_dims_usize)));

        self.read_into_async::<T>(
            out.clone(),
            dim_read,
            &vec![0; dim_read.len()],
            &out_dims,
            io_size_max,
            io_size_merge,
        )
        .await?;

        let out = Arc::into_inner(out).expect("Failed to unwrap Arc");
        Ok(out.into_inner())
    }

    /// Read into an existing array asynchronously with concurrent processing
    pub async fn read_into_async<T: OmFileArrayDataType + Send + Sync + 'static>(
        &self,
        into: Arc<SharedArray<T>>,
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

        // Validate dimensions
        let n_dimensions_read = dim_read.len();
        let n_dims = self.get_dimensions().len();
        if n_dims != n_dimensions_read
            || n_dimensions_read != into_cube_offset.len()
            || n_dimensions_read != into_cube_dimension.len()
        {
            return Err(OmFilesRsError::MismatchingCubeDimensionLength);
        }

        // Prepare read parameters
        let read_offset: Vec<u64> = dim_read.iter().map(|r| r.start).collect();
        let read_count: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();

        // Initialize decoder, decoder can be shared across tasks via Arc
        let decoder = Arc::new(DecoderWrapper::new(
            self.variable,
            n_dimensions_read as u64,
            &read_offset,
            &read_count,
            into_cube_offset,
            into_cube_dimension,
            io_size_merge,
            io_size_max,
        )?);

        // Semaphore to limit concurrency
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENCY.get()));

        // Use a vector to collect task handles
        let mut tasks = Vec::new();

        // Process all index blocks
        let mut index_read = new_index_read(&decoder.decoder);
        while decoder.next_index_read(&mut index_read) {
            // Fetch index data in a blocking task
            let index_data = {
                // Acquire permit, limiting concurrency
                let _permit = semaphore.acquire().await;
                get_bytes_async(&self.backend.clone(), index_read.offset, index_read.count).await?
            };

            // Create a collection to store single chunks to process
            let mut chunk_tasks = Vec::new();

            // Collect tasks from the callback without spawning them
            decoder.process_data_reads(
                &index_read,
                &index_data,
                |offset, count, chunk_index| {
                    // Collect task parameters for later processing
                    chunk_tasks.push((offset, count, chunk_index));
                    Ok(())
                },
            )?;

            // Spawn tasks after collecting all parameters
            for (offset, count, chunk_index) in chunk_tasks {
                let decoder_clone = decoder.clone();
                let into_clone = into.clone();
                let backend_clone = self.backend.clone();
                let semaphore_clone = semaphore.clone();

                tasks.push(get_executor().spawn(async move {
                    // Acquire permit limiting concurrency
                    let permit = semaphore_clone.acquire_arc().await;
                    let data_bytes = get_bytes_async(&backend_clone, offset, count).await?;

                    // Process the chunk
                    let result = ChunkDecodeTask {
                        chunk_index,
                        data_bytes,
                        output: into_clone,
                        decoder: decoder_clone,
                    }
                    .process()
                    .await;

                    // Release the permit by dropping it explicitly
                    drop(permit);

                    result
                }));
            }
        }

        // Wait for all tasks to complete
        let mut encountered_error = None;

        // Run the executor with our collected tasks
        get_executor()
            .run(async {
                for task in tasks {
                    match task.await {
                        Ok(()) => {} // Task completed successfully
                        Err(e) => {
                            // Store first error encountered
                            if encountered_error.is_none() {
                                encountered_error = Some(e);
                            }
                        }
                    }
                }
            })
            .await;

        // Return error if any occurred
        if let Some(e) = encountered_error {
            return Err(e);
        }

        Ok(())
    }
}

/// Asynchronously fetches a byte range from a backend storage.
///
/// This function provides an async interface to the underlying backend's byte retrieval
/// capabilities. Currently, it works by dispatching blocking I/O operations
/// on a `smol::blocking` thread pool.
///
/// # Implementation Details
///
/// The function tries two strategies in sequence:
/// 1. First attempts `get_bytes_owned` to obtain ownership of the data
/// 2. Falls back to `get_bytes` with a copy operation if the backend doesn't support the first method
async fn get_bytes_async<Backend: OmFileReaderBackend + Send + Sync + 'static>(
    backend: &Arc<Backend>,
    offset: u64,
    count: u64,
) -> Result<Vec<u8>, OmFilesRsError> {
    // Try get_bytes_owned first in a blocking task
    match unblock({
        let backend = backend.clone();
        move || backend.get_bytes_owned(offset, count)
    })
    .await
    {
        Ok(data) => Ok(data),
        Err(error) => {
            // If get_bytes_owned failed with NotImplementedError, try get_bytes as fallback
            match error {
                OmFilesRsError::NotImplementedError(_) => {
                    // Get bytes directly using get_bytes
                    // This must run in a blocking task since get_bytes returns a reference
                    // We need to copy the data to return an owned Vec<u8>
                    unblock({
                        let backend = backend.clone();
                        move || backend.get_bytes(offset, count).map(|bytes| bytes.to_vec())
                    })
                    .await
                }
                _ => Err(error), // Return original error if it's not NotImplementedError
            }
        }
    }
}

struct DecoderWrapper {
    decoder: OmDecoder_t,
}

unsafe impl Send for DecoderWrapper {}
unsafe impl Sync for DecoderWrapper {}

impl DecoderWrapper {
    /// Initialize the decoder with read parameters
    fn new(
        variable: OmVariablePtr,
        dims: u64,
        read_offset: &[u64],
        read_count: &[u64],
        cube_offset: &[u64],
        cube_dim: &[u64],
        io_size_merge: u64,
        io_size_max: u64,
    ) -> Result<Self, OmFilesRsError> {
        let mut decoder = unsafe { create_uninit_decoder() };
        let error = unsafe {
            om_decoder_init(
                &mut decoder,
                *variable,
                dims,
                read_offset.as_ptr(),
                read_count.as_ptr(),
                cube_offset.as_ptr(),
                cube_dim.as_ptr(),
                io_size_merge,
                io_size_max,
            )
        };

        if error != OmError_t::ERROR_OK {
            let error_string = c_error_string(error);
            return Err(OmFilesRsError::DecoderError(error_string));
        }

        Ok(Self { decoder })
    }

    /// Get the required buffer size for decoding
    fn buffer_size(&self) -> usize {
        unsafe { om_decoder_read_buffer_size(&self.decoder) as usize }
    }

    /// Decode a chunk safely
    fn decode_chunk(
        &self,
        chunk_index: OmRange_t,
        data: &[u8],
        output: &mut [u8], // Raw bytes of output array
        chunk_buffer: &mut [u8],
    ) -> Result<(), OmFilesRsError> {
        let mut error = OmError_t::ERROR_OK;

        let success = unsafe {
            om_decoder_decode_chunks(
                &self.decoder,
                chunk_index,
                data.as_ptr() as *const c_void,
                data.len() as u64,
                output.as_mut_ptr() as *mut c_void,
                chunk_buffer.as_mut_ptr() as *mut c_void,
                &mut error,
            )
        };

        if !success {
            let error_string = c_error_string(error);
            return Err(OmFilesRsError::DecoderError(error_string));
        }

        Ok(())
    }

    /// Process the next index block
    fn next_index_read(&self, index_read: &mut OmDecoder_indexRead_t) -> bool {
        unsafe { om_decoder_next_index_read(&self.decoder, index_read) }
    }

    /// Process data reads for an index block
    fn process_data_reads<F>(
        &self,
        index_read: &OmDecoder_indexRead_t,
        index_data: &[u8],
        mut callback: F,
    ) -> Result<(), OmFilesRsError>
    where
        F: FnMut(u64, u64, OmRange_t) -> Result<(), OmFilesRsError>,
    {
        let mut data_read = new_data_read(index_read);
        let mut error = OmError_t::ERROR_OK;

        while unsafe {
            om_decoder_next_data_read(
                &self.decoder,
                &mut data_read,
                index_data.as_ptr() as *const c_void,
                index_data.len() as u64,
                &mut error,
            )
        } {
            if error != OmError_t::ERROR_OK {
                let error_string = c_error_string(error);
                return Err(OmFilesRsError::DecoderError(error_string));
            }
            // Pass relevant data to the callback
            callback(data_read.offset, data_read.count, data_read.chunkIndex)?;
        }

        Ok(())
    }
}

/// A wrapper around ArrayD that allows concurrent access to disjoint regions
///
/// # Safety
///
/// This type allows multiple tasks to access different parts of the same array simultaneously,
/// which is not normally allowed in Rust. It is safe because:
///
/// 1. The OM decoder guarantees that each chunk writes to a disjoint region of the output array
/// 2. We never allow direct mutable access to the array from multiple threads
/// 3. All modifications happen through the C decoder which respects the boundaries
pub struct SharedArray<T> {
    inner: UnsafeCell<ArrayD<T>>,
}

unsafe impl<T: Send> Sync for SharedArray<T> {}

impl<T> SharedArray<T> {
    fn new(array: ArrayD<T>) -> Self {
        Self {
            inner: UnsafeCell::new(array),
        }
    }

    fn into_inner(self) -> ArrayD<T> {
        self.inner.into_inner()
    }
}

struct ChunkDecodeTask<T: OmFileArrayDataType> {
    chunk_index: OmRange_t,
    data_bytes: Vec<u8>,
    output: Arc<SharedArray<T>>,
    decoder: Arc<DecoderWrapper>,
}

impl<T: OmFileArrayDataType + Send + Sync + 'static> ChunkDecodeTask<T> {
    async fn process(self) -> Result<(), OmFilesRsError> {
        // Output buffer for decoding, could potentially be fetched from a pool
        let mut chunk_buffer = vec![0u8; self.decoder.buffer_size()];

        // SAFETY: We rely on the C decoder to ensure each chunk writes to disjoint regions
        let output_bytes = unsafe {
            let array = &mut *self.output.inner.get();
            let output_slice = array
                .as_slice_mut()
                .ok_or(OmFilesRsError::ArrayNotContiguous)?;

            std::slice::from_raw_parts_mut(
                output_slice.as_mut_ptr() as *mut u8,
                output_slice.len() * std::mem::size_of::<T>(),
            )
        };

        // Decode the chunk in a blocking task
        unblock(move || {
            self.decoder.decode_chunk(
                self.chunk_index,
                &self.data_bytes,
                output_bytes,
                &mut chunk_buffer,
            )
        })
        .await
    }
}
