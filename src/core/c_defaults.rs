use omfileformatc_rs::{
    om_decoder_init_data_read, om_decoder_init_index_read, om_error_string, OmDecoder_dataRead_t,
    OmDecoder_indexRead_t, OmDecoder_t, OmEncoder_t, OmError_t, OmRange_t,
};

pub fn create_decoder() -> OmDecoder_t {
    OmDecoder_t {
        dimensions_count: 0,
        io_size_merge: 0,
        io_size_max: 0,
        lut_chunk_length: 0,
        lut_start: 0,
        number_of_chunks: 0,
        dimensions: std::ptr::null_mut(),
        chunks: std::ptr::null_mut(),
        read_offset: std::ptr::null_mut(),
        read_count: std::ptr::null_mut(),
        cube_offset: std::ptr::null_mut(),
        cube_dimensions: std::ptr::null_mut(),
        decompress_callback: None,
        decompress_filter_callback: None,
        decompress_copy_callback: None,
        scale_factor: 0.0,
        add_offset: 0.0,
        bytes_per_element: 0,
        bytes_per_element_compressed: 0,
    }
}

pub fn create_encoder() -> OmEncoder_t {
    OmEncoder_t {
        dimension_count: 0,
        dimensions: std::ptr::null_mut(),
        chunks: std::ptr::null_mut(),
        compress_callback: None,
        compress_filter_callback: None,
        compress_copy_callback: None,
        scale_factor: 0.0,
        add_offset: 0.0,
        bytes_per_element: 0,
        bytes_per_element_compressed: 0,
    }
}

pub fn new_index_read(decoder: &OmDecoder_t) -> OmDecoder_indexRead_t {
    let mut index_read = OmDecoder_indexRead_t {
        offset: 0,
        count: 0,
        indexRange: OmRange_t {
            lowerBound: 0,
            upperBound: 0,
        },
        chunkIndex: OmRange_t {
            lowerBound: 0,
            upperBound: 0,
        },
        nextChunk: OmRange_t {
            lowerBound: 0,
            upperBound: 0,
        },
    };
    unsafe { om_decoder_init_index_read(decoder, &mut index_read) };
    index_read
}

pub fn new_data_read(index_read: &OmDecoder_indexRead_t) -> OmDecoder_dataRead_t {
    let mut data_read = OmDecoder_dataRead_t {
        offset: 0,
        count: 0,
        indexRange: OmRange_t {
            lowerBound: 0,
            upperBound: 0,
        },
        chunkIndex: OmRange_t {
            lowerBound: 0,
            upperBound: 0,
        },
        nextChunk: OmRange_t {
            lowerBound: 0,
            upperBound: 0,
        },
    };
    unsafe { om_decoder_init_data_read(&mut data_read, index_read) };
    data_read
}

pub fn c_error_string(error: OmError_t) -> String {
    let ptr = unsafe { om_error_string(error) };
    let error_string = unsafe { std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned() };
    error_string
}
