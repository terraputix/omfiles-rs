#![allow(non_snake_case)]
use crate::backend::backends::OmFileReaderBackendAsync;
use crate::core::data_types::OmFileArrayDataType;
use crate::errors::OmFilesRsError;
use crate::io::reader::OmFileReader;
use async_executor::{Executor, Task};
use async_lock::Semaphore;
use ndarray::ArrayD;
use num_traits::Zero;
use om_file_format_sys::OmRange_t;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

static EXECUTOR: OnceLock<Executor> = OnceLock::new();
fn get_executor() -> &'static Executor<'static> {
    EXECUTOR.get_or_init(|| Executor::new())
}

/// Maximum number of concurrent tasks for async operations
/// TODO: this could potentially be moved to the reader level
const MAX_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(16).unwrap();

impl<Backend: OmFileReaderBackendAsync + Send + Sync + 'static> OmFileReader<Backend> {
    /// Read a variable asynchronously, using concurrent fetching and decoding
    pub async fn read_async<T: OmFileArrayDataType + Clone + Zero + Send + Sync + 'static>(
        &self,
        dim_read: &[Range<u64>],
        io_size_max: Option<u64>,
        io_size_merge: Option<u64>,
    ) -> Result<ArrayD<T>, OmFilesRsError> {
        let out_dims: Vec<u64> = dim_read.iter().map(|r| r.end - r.start).collect();
        let out_dims_usize = out_dims.iter().map(|&x| x as usize).collect::<Vec<_>>();

        let mut out = ArrayD::<T>::zeros(out_dims_usize);

        self.read_into_async::<T>(
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

    /// Read into an existing array asynchronously with concurrent processing
    pub async fn read_into_async<T: OmFileArrayDataType + Send + Sync + 'static>(
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
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENCY.get()));

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
