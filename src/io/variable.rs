use om_file_format_sys::OmVariable_t;
use std::ops::Deref;

/// A wrapper around the raw C pointer OmVariable_t
/// marked as Send + Sync.
///
/// # Safety
///
/// This relies on the assumption that the underlying C library functions
/// used for reading metadata via this pointer (`om_variable_get_*`) are
/// thread-safe when called concurrently on the same immutable variable data.
/// The pointer itself points into the `variable_data` Vec owned by the
/// `OmFileReader`, ensuring its validity for the lifetime of the reader instance.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OmVariablePtr(pub(crate) *const OmVariable_t);

/// SAFETY: See safety note above. We assert that read-only access via this pointer
/// is safe to perform concurrently from multiple threads, provided the underlying
/// `variable_data` remains valid and unchanged, which is guaranteed by `OmFileReader`'s ownership.
unsafe impl Send for OmVariablePtr {}
unsafe impl Sync for OmVariablePtr {}

impl Deref for OmVariablePtr {
    type Target = *const OmVariable_t;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
