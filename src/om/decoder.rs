use omfileformatc_rs::{
    om_decoder_data_read_init, om_decoder_data_read_t, om_decoder_index_read_init,
    om_decoder_index_read_t, om_decoder_t, om_range_t,
};

pub fn create_decoder() -> om_decoder_t {
    om_decoder_t {
        dimensions_count: 0,
        io_size_merge: 0,
        io_size_max: 0,
        lut_chunk_length: 0,
        lut_chunk_element_count: 0,
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
        scalefactor: 0.0,
        bytes_per_element: 0,
    }
}

pub fn new_index_read(decoder: &om_decoder_t) -> om_decoder_index_read_t {
    let mut index_read = om_decoder_index_read_t {
        offset: 0,
        count: 0,
        indexRange: om_range_t {
            lowerBound: 0,
            upperBound: 0,
        },
        chunkIndex: om_range_t {
            lowerBound: 0,
            upperBound: 0,
        },
        nextChunk: om_range_t {
            lowerBound: 0,
            upperBound: 0,
        },
    };
    unsafe { om_decoder_index_read_init(decoder, &mut index_read) };
    index_read
}

pub fn new_data_read(index_read: &om_decoder_index_read_t) -> om_decoder_data_read_t {
    let mut data_read = om_decoder_data_read_t {
        offset: 0,
        count: 0,
        indexRange: om_range_t {
            lowerBound: 0,
            upperBound: 0,
        },
        chunkIndex: om_range_t {
            lowerBound: 0,
            upperBound: 0,
        },
        nextChunk: om_range_t {
            lowerBound: 0,
            upperBound: 0,
        },
    };
    unsafe { om_decoder_data_read_init(&mut data_read, index_read) };
    data_read
}
