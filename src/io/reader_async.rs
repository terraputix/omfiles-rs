use crate::backend::backends::OmFileReaderBackendAsync;
use crate::core::data_types::OmFileArrayDataType;
use crate::errors::OmFilesRsError;
use crate::implement_variable_methods;
use async_executor::{Executor, Task};
use async_lock::Semaphore;
use ndarray::ArrayD;
use num_traits::Zero;
use om_file_format_sys::{
    om_header_size, om_header_type, om_trailer_size, OmHeaderType_t, OmRange_t,
};
use std::ffi::c_void;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use super::reader_utils::process_trailer;
use super::variable::OmVariableContainer;

static EXECUTOR: OnceLock<Executor> = OnceLock::new();
fn get_executor() -> &'static Executor<'static> {
    EXECUTOR.get_or_init(|| Executor::new())
}

pub struct OmFileReaderAsync<Backend> {
    pub backend: Arc<Backend>,
    variable: OmVariableContainer,
    max_concurrency: NonZeroUsize,
}

// implement utility methods for OmFileReaderAsync
implement_variable_methods!(OmFileReaderAsync<Backend>);

impl<Backend: OmFileReaderBackendAsync + Send + Sync + 'static> OmFileReaderAsync<Backend> {
    pub async fn new(backend: Arc<Backend>) -> Result<Self, OmFilesRsError> {
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

                    let offset_size = process_trailer(&trailer_data)?;
                    let variable_data = backend
                        .get_bytes_async(offset_size.offset, offset_size.size)
                        .await?;
                    (variable_data, Some(offset_size))
                },
                OmHeaderType_t::OM_HEADER_INVALID => {
                    return Err(OmFilesRsError::NotAnOmFile);
                }
            }
        };

        Ok(Self {
            backend,
            variable: OmVariableContainer::new(variable_data, offset_size),
            max_concurrency: NonZeroUsize::new(16).unwrap(),
        })
    }

    pub fn set_max_concurrency(&mut self, max_concurrency: NonZeroUsize) {
        self.max_concurrency = max_concurrency;
    }

    /// Read a variable asynchronously using concurrent fetches
    /// The decoding is still done sequentially
    pub async fn read<T: OmFileArrayDataType + Clone + Zero + Send + Sync + 'static>(
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
        )
        .await?;

        Ok(out)
    }

    /// Read into an existing array asynchronously using concurrent fetches
    /// The decoding is still done sequentially
    pub async fn read_into<T: OmFileArrayDataType + Send + Sync + 'static>(
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

        // Semaphore to limit concurrency
        let semaphore = Arc::new(Semaphore::new(self.max_concurrency.get()));

        // Process all index blocks
        let mut index_read = decoder.new_index_read();
        while decoder.next_index_read(&mut index_read) {
            // Acquire permit, limiting concurrency
            let _permit = semaphore.acquire().await;
            // Fetch index data in a blocking task
            let index_data = self
                .backend
                .get_bytes_async(index_read.offset, index_read.count)
                .await?;
            drop(_permit);

            // Create a collection to store single chunks to process
            let mut chunk_infos = Vec::new();
            // Collect tasks from the callback without spawning them
            decoder.process_data_reads(
                &index_read,
                &index_data,
                |offset, count, chunk_index| {
                    // Collect task parameters for later processing
                    chunk_infos.push((offset, count, chunk_index));
                    Ok(())
                },
            )?;

            let mut task_handles: Vec<Task<Result<(Vec<u8>, OmRange_t), OmFilesRsError>>> =
                Vec::with_capacity(chunk_infos.len());

            // Spawn a task for each chunk info
            for (offset, count, chunk_index) in chunk_infos {
                let backend = self.backend.clone();
                let semaphore_clone = semaphore.clone();

                let task = get_executor().spawn(async move {
                    // Acquire permit limiting concurrency
                    let permit = semaphore_clone.acquire_arc().await;

                    // Fetch data and attach chunk index
                    let data = backend.get_bytes_async(offset, count).await?;
                    let result = Ok((data, chunk_index));

                    // Release permit
                    drop(permit);

                    result
                });
                task_handles.push(task);
            }

            // Run the executor to process all tasks
            let mut chunk_data: Vec<(Vec<u8>, OmRange_t)> = Vec::with_capacity(task_handles.len());
            get_executor()
                .run(async {
                    for handle in task_handles {
                        match handle.await {
                            Ok(result) => chunk_data.push(result),
                            Err(e) => return Err(OmFilesRsError::TaskError(e.to_string())),
                        }
                    }
                    Ok::<_, OmFilesRsError>(())
                })
                .await?;

            // Decode all chunks sequentially.
            // This could also potentially be parallelized using a thread pool.
            let mut chunk_buffer = vec![0u8; decoder.buffer_size()];
            // Get access to the output array
            // SAFETY: The decoder is supposed to write into disjoint slices
            // of the output array, so this is not racy!
            let output_bytes = unsafe {
                let output_slice = into
                    .as_slice_mut()
                    .ok_or(OmFilesRsError::ArrayNotContiguous)?;

                std::slice::from_raw_parts_mut(
                    output_slice.as_mut_ptr() as *mut u8,
                    output_slice.len() * std::mem::size_of::<T>(),
                )
            };
            let results: Vec<Result<(), OmFilesRsError>> = chunk_data
                .into_iter()
                .map(|(data_bytes, chunk_index)| {
                    decoder.decode_chunk(chunk_index, &data_bytes, output_bytes, &mut chunk_buffer)
                })
                .collect();

            // Check for errors
            for result in results {
                if let Err(e) = result {
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}
