use omfileformatc_rs::om_decoder_t;

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
