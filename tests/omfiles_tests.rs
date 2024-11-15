use omfileformatc_rs::{fpxdec32, fpxenc32};

use omfiles_rs::{
    compression::{p4ndec256_bound, p4nenc256_bound, CompressionType},
    data_types::DataType,
    om::{
        backends::OmFileReaderBackend,
        errors::OmFilesRsError,
        mmapfile::{MmapFile, Mode},
        omfile_json::OmFileJSON,
        reader::OmFileReader,
        reader2::OmFileReader2,
        write_buffer::OmWriteBuffer,
        writer::OmFileWriter,
        writer2::{OmFileWriter2, OmFileWriterArray},
    },
};

use std::{
    borrow::BorrowMut,
    f32,
    fs::{self, File},
    sync::Arc,
};

#[test]
fn turbo_pfor_roundtrip() {
    let data: Vec<f32> = vec![10.0, 22.0, 23.0, 24.0];
    let length = 1; //data.len();

    // create buffers for compression and decompression!
    let compressed_buffer = vec![0; p4nenc256_bound(length, 4)];
    let compressed = compressed_buffer.as_slice();
    let decompress_buffer = vec![0.0; p4ndec256_bound(length, 4)];
    let decompressed = decompress_buffer.as_slice();

    // compress data
    let compressed_size = unsafe {
        fpxenc32(
            data.as_ptr() as *mut u32,
            length,
            compressed.as_ptr() as *mut u8,
            0,
        )
    };
    if compressed_size >= compressed.len() {
        panic!("Compress Buffer too small");
    }

    // decompress data
    let decompressed_size = unsafe {
        fpxdec32(
            compressed.as_ptr() as *mut u8,
            length,
            decompressed.as_ptr() as *mut u32,
            0,
        )
    };
    if decompressed_size >= decompressed.len() {
        panic!("Decompress Buffer too small");
    }

    // this should be equal (we check it in the reader)
    // here we have a problem if length is only 1 and the exponent of the
    // float is greater than 0 (e.g. the value is greater than 10)
    // NOTE: This fails with 4 != 5
    assert_eq!(decompressed_size, compressed_size);
    assert_eq!(data[..length], decompressed[..length]);
}

#[test]
fn test_write_empty_array_throws() -> Result<(), Box<dyn std::error::Error>> {
    let data: Vec<f32> = vec![];
    let compressed =
        OmFileWriter::new(0, 0, 0, 0).write_all_in_memory(CompressionType::P4nzdec256, 1.0, &data);
    // make sure there was an error and it is of the correct type
    assert!(compressed.is_err());
    let err = compressed.err().unwrap();
    // make sure the error is of the correct type
    assert_eq!(err, OmFilesRsError::DimensionMustBeLargerThan0);

    Ok(())
}

#[test]
fn test_in_memory_int_compression() -> Result<(), Box<dyn std::error::Error>> {
    let data: Vec<f32> = vec![
        0.0, 5.0, 2.0, 3.0, 2.0, 5.0, 6.0, 2.0, 8.0, 3.0, 10.0, 14.0, 12.0, 15.0, 14.0, 15.0, 66.0,
        17.0, 12.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
    ];
    let must_equal = data.clone();
    let compressed = OmFileWriter::new(1, data.len(), 1, 10).write_all_in_memory(
        CompressionType::P4nzdec256,
        1.0,
        &data,
    )?;

    assert_eq!(compressed.count(), 212);

    let uncompressed = OmFileReader::new(compressed)
        .expect("Could not get data from backend")
        .read_all()?;

    assert_eq_with_accuracy(&must_equal, &uncompressed, 0.001);

    Ok(())
}

#[test]
fn test_in_memory_f32_compression() -> Result<(), Box<dyn std::error::Error>> {
    let data: Vec<f32> = vec![
        0.0, 5.0, 2.0, 3.0, 2.0, 5.0, 6.0, 2.0, 8.0, 3.0, 10.0, 14.0, 12.0, 15.0, 14.0, 15.0, 66.0,
        17.0, 12.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
    ];
    let must_equal = data.clone();
    let compressed = OmFileWriter::new(1, data.len(), 1, 10).write_all_in_memory(
        CompressionType::Fpxdec32,
        1.0,
        &data,
    )?;

    assert_eq!(compressed.count(), 236);

    let uncompressed = OmFileReader::new(compressed)
        .expect("Could not get data from backend")
        .read_all()?;

    assert_eq_with_accuracy(&must_equal, &uncompressed, 0.001);

    Ok(())
}

#[test]
fn test_write_more_data_than_expected() -> Result<(), Box<dyn std::error::Error>> {
    let file = "writetest_failing.om";
    remove_file_if_exists(file);

    let result0 = Arc::new((0..10).map(|x| x as f32).collect::<Vec<f32>>());
    let result2 = Arc::new((10..20).map(|x| x as f32).collect::<Vec<f32>>());
    let result4 = Arc::new((20..30).map(|x| x as f32).collect::<Vec<f32>>());

    // Attempt to write more data than expected and ensure it throws an error
    let result = OmFileWriter::new(5, 5, 2, 2).write_to_file(
        file,
        CompressionType::P4nzdec256,
        1.0,
        false,
        |dim0pos| match dim0pos {
            0 => Ok(result0.as_slice()),
            2 => Ok(result2.as_slice()),
            4 => Ok(result4.as_slice()),
            _ => panic!("Not expected"),
        },
    );

    // Ensure that an error was thrown
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err, OmFilesRsError::ChunkHasWrongNumberOfElements);

    // Remove the temporary file if it exists
    let temp_file = format!("{}~", file);
    remove_file_if_exists(&temp_file);

    Ok(())
}

#[test]
fn test_write_large() -> Result<(), Box<dyn std::error::Error>> {
    let file = "writetest.om";
    std::fs::remove_file(file).ok();

    let mut writer = OmFileWriterArray::new(
        vec![100, 100, 10],
        vec![2, 2, 2],
        CompressionType::P4nzdec256,
        DataType::Float,
        1.0,
        0.0,
        256,
    );
    let mut buffer = OmWriteBuffer::new(1);

    let mut file_handle = File::create(file)?;
    let mut file_handle = file_handle.borrow_mut();

    let data: Vec<f32> = (0..100000).map(|x| (x % 10000) as f32).collect();
    OmFileWriter2::write_header(&mut buffer);
    writer.write_data(
        &data,
        &[100, 100, 10],
        &[0..100, 0..100, 0..10],
        &mut file_handle,
        &mut buffer,
    )?;
    let json_variable = writer.compress_lut_and_return_meta(&mut buffer);
    let json = OmFileJSON {
        variables: vec![json_variable],
        some_attributes: None,
    };
    OmFileWriter2::write_trailer(&mut buffer, &json)?;

    buffer.write_to_file(&mut file_handle)?;

    let file_for_reading = File::open(file)?;
    let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;

    let read = OmFileReader2::open_file(read_backend, 256)?;

    let a1 = read.read_simple(&[50..51, 20..21, 1..2], 65536, 512)?;
    assert_eq!(a1, vec![201.0]);

    let a = read.read_simple(&[0..100, 0..100, 0..10], 65536, 512)?;
    assert_eq!(a.len(), data.len());
    let range = 0..100; // a.len() - 100..a.len() - 1
    assert_eq!(a[range.clone()], data[range]);

    Ok(())
}

#[test]
fn test_write_chunks() -> Result<(), Box<dyn std::error::Error>> {
    let file = "writetest.om";
    remove_file_if_exists(file);

    let mut writer = OmFileWriterArray::new(
        vec![5, 5],
        vec![2, 2],
        CompressionType::P4nzdec256,
        DataType::Float,
        1.0,
        0.0,
        256,
    );

    let mut buffer = OmWriteBuffer::new(1);

    let mut file_handle = File::create(file)?;
    let mut file_handle = file_handle.borrow_mut();

    OmFileWriter2::write_header(&mut buffer);

    // Directly feed individual chunks
    writer.write_data(
        &[0.0, 1.0, 5.0, 6.0],
        &[2, 2],
        &[0..2, 0..2],
        &mut file_handle,
        &mut buffer,
    )?;
    writer.write_data(
        &[2.0, 3.0, 7.0, 8.0],
        &[2, 2],
        &[0..2, 0..2],
        &mut file_handle,
        &mut buffer,
    )?;
    writer.write_data(
        &[4.0, 9.0],
        &[2, 1],
        &[0..2, 0..1],
        &mut file_handle,
        &mut buffer,
    )?;
    writer.write_data(
        &[10.0, 11.0, 15.0, 16.0],
        &[2, 2],
        &[0..2, 0..2],
        &mut file_handle,
        &mut buffer,
    )?;
    writer.write_data(
        &[12.0, 13.0, 17.0, 18.0],
        &[2, 2],
        &[0..2, 0..2],
        &mut file_handle,
        &mut buffer,
    )?;
    writer.write_data(
        &[14.0, 19.0],
        &[2, 1],
        &[0..2, 0..1],
        &mut file_handle,
        &mut buffer,
    )?;
    writer.write_data(
        &[20.0, 21.0],
        &[1, 2],
        &[0..1, 0..2],
        &mut file_handle,
        &mut buffer,
    )?;
    writer.write_data(
        &[22.0, 23.0],
        &[1, 2],
        &[0..1, 0..2],
        &mut file_handle,
        &mut buffer,
    )?;
    writer.write_data(
        &[24.0],
        &[1, 1],
        &[0..1, 0..1],
        &mut file_handle,
        &mut buffer,
    )?;

    let json_variable = writer.compress_lut_and_return_meta(&mut buffer);
    let json = OmFileJSON {
        variables: vec![json_variable],
        some_attributes: None,
    };
    OmFileWriter2::write_trailer(&mut buffer, &json)?;
    buffer.write_to_file(&mut file_handle)?;

    // test reading
    let file_for_reading = File::open(file)?;
    let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;

    let file = OmFileReader2::open_file(read_backend, 256)?;
    let variables = file.get_variables();
    let read = &variables[0];

    let a = read.read(&[0..5, 0..5], 65536, 512);
    let expected = vec![
        0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
        17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
    ];
    assert_eq!(a, expected);

    Ok(())
}

#[test]
fn test_offset_write() -> Result<(), Box<dyn std::error::Error>> {
    let file = "writetest.om";
    remove_file_if_exists(file);

    // Set up the writer with the specified dimensions and chunk dimensions
    let dimensions = vec![5, 5];
    let chunk_dimensions = vec![2, 2];
    let compression = CompressionType::P4nzdec256;
    let data_type = DataType::Float;
    let scale_factor = 1.0;
    let add_offset = 0.0;
    let lut_chunk_element_count = 256;

    // Create the writer
    let mut writer = OmFileWriterArray::new(
        dimensions.clone(),
        chunk_dimensions,
        compression,
        data_type,
        scale_factor,
        add_offset,
        lut_chunk_element_count,
    );

    // Create the write buffer with initial capacity of 1
    let mut buffer = OmWriteBuffer::new(1);

    // Create the file handle
    let mut file_handle = File::create(file)?;
    let mut file_handle = file_handle.borrow_mut();

    // Deliberately add NaN on all positions that should not be written to the file.
    // Only the inner 5x5 array is written.
    let data = vec![
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        0.0,
        1.0,
        2.0,
        3.0,
        4.0,
        f32::NAN,
        f32::NAN,
        5.0,
        6.0,
        7.0,
        8.0,
        9.0,
        f32::NAN,
        f32::NAN,
        10.0,
        11.0,
        12.0,
        13.0,
        14.0,
        f32::NAN,
        f32::NAN,
        15.0,
        16.0,
        17.0,
        18.0,
        19.0,
        f32::NAN,
        f32::NAN,
        20.0,
        21.0,
        22.0,
        23.0,
        24.0,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
        f32::NAN,
    ];

    // Write header
    OmFileWriter2::write_header(&mut buffer);

    // Write data with array dimensions [7,7] and reading from [1..6, 1..6]
    writer.write_data(&data, &[7, 7], &[1..6, 1..6], &mut file_handle, &mut buffer)?;

    // Compress LUT and get metadata
    let json_variable = writer.compress_lut_and_return_meta(&mut buffer);
    let json = OmFileJSON {
        variables: vec![json_variable],
        some_attributes: None,
    };

    // Write trailer
    OmFileWriter2::write_trailer(&mut buffer, &json)?;

    // Write buffer to file
    buffer.write_to_file(&mut file_handle)?;

    // Read the file
    let file_for_reading = File::open(file)?;
    let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
    let reader = OmFileReader2::open_file(read_backend, 256)?;
    let variables = reader.get_variables();
    let read_var = &variables[0];

    // Read the data
    let a = read_var.read(&[0..5, 0..5], 65536, 512);

    // Expected data
    let expected = vec![
        0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
        17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
    ];

    assert_eq!(a, expected);

    Ok(())
}

#[test]
fn test_write_3d() -> Result<(), Box<dyn std::error::Error>> {
    let file = "writetest.om";
    remove_file_if_exists(file);

    let dims = vec![3, 3, 3];
    let chunk_dimensions = vec![2, 2, 2];
    let compression = CompressionType::P4nzdec256;
    let data_type = DataType::Float;
    let scale_factor = 1.0;
    let add_offset = 0.0;
    let lut_chunk_element_count = 256;

    let mut writer = OmFileWriterArray::new(
        dims.clone(),
        chunk_dimensions,
        compression,
        data_type,
        scale_factor,
        add_offset,
        lut_chunk_element_count,
    );

    let mut buffer = OmWriteBuffer::new(1);
    let mut file_handle = File::create(file)?;
    let mut file_handle = file_handle.borrow_mut();

    let data: Vec<f32> = (0..27).map(|x| x as f32).collect();

    // Write header
    OmFileWriter2::write_header(&mut buffer);

    // Write data
    writer.write_data(
        &data,
        &[3, 3, 3],
        &[0..3, 0..3, 0..3],
        &mut file_handle,
        &mut buffer,
    )?;

    // Compress LUT and get metadata
    let json_variable = writer.compress_lut_and_return_meta(&mut buffer);
    let json = OmFileJSON {
        variables: vec![json_variable],
        some_attributes: None,
    };

    // Write trailer
    OmFileWriter2::write_trailer(&mut buffer, &json)?;

    // Write buffer to file
    buffer.write_to_file(&mut file_handle)?;

    // Read the file
    let file_for_reading = File::open(file)?;
    let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;
    let reader = OmFileReader2::open_file(read_backend, lut_chunk_element_count)?;
    let variables = reader.get_variables();
    let read_var = &variables[0];

    // Read the data
    let a = read_var.read(&[0..3, 0..3, 0..3], 65536, 512);

    // Check if read data matches original data
    assert_eq!(a, data);

    // Single index checks
    for x in 0..dims[0] {
        for y in 0..dims[1] {
            for z in 0..dims[2] {
                let value = read_var.read(&[x..x + 1, y..y + 1, z..z + 1], 65536, 512);
                assert_eq!(value, vec![(x * 9 + y * 3 + z) as f32]);
            }
        }
    }

    Ok(())
}

#[test]
fn test_write_v3() -> Result<(), Box<dyn std::error::Error>> {
    let file = "writetest.om";
    remove_file_if_exists(file);

    // Define dimensions and chunk dimensions
    let dims = vec![5, 5];
    let chunk_dimensions = vec![2, 2];
    let compression = CompressionType::P4nzdec256;
    let data_type = DataType::Float;
    let scale_factor = 1.0;
    let add_offset = 0.0;
    let lut_chunk_element_count = 2;

    // Initialize the writer
    let mut writer = OmFileWriterArray::new(
        dims.clone(),
        chunk_dimensions,
        compression,
        data_type,
        scale_factor,
        add_offset,
        lut_chunk_element_count,
    );

    // Initialize the write buffer with a capacity of 1
    let mut buffer = OmWriteBuffer::new(1);

    // Create the file handle for writing
    let mut file_handle = File::create(file)?;
    let mut file_handle = file_handle.borrow_mut();

    // Define the data to write
    let data: Vec<f32> = (0..25).map(|x| x as f32).collect();

    // Write header to the buffer
    OmFileWriter2::write_header(&mut buffer);

    // Write data to the buffer with array dimensions [5,5] and read ranges [0..5, 0..5]
    writer.write_data(&data, &[5, 5], &[0..5, 0..5], &mut file_handle, &mut buffer)?;

    // Compress LUT and retrieve metadata
    let json_variable = writer.compress_lut_and_return_meta(&mut buffer);
    let json = OmFileJSON {
        variables: vec![json_variable],
        some_attributes: None,
    };

    // Write trailer to the buffer
    OmFileWriter2::write_trailer(&mut buffer, &json)?;

    // Flush the buffer contents to the file
    buffer.write_to_file(&mut file_handle)?;

    // Open the file for reading
    let file_for_reading = File::open(file)?;
    let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;

    // Initialize the reader using the open_file method
    let reader = OmFileReader2::open_file(read_backend, lut_chunk_element_count)?;
    let variables = reader.get_variables();
    let read_var = &variables[0];

    // Read the entire data back and assert equality
    let a = read_var.read(&[0..5, 0..5], 65536, 512);
    let expected = vec![
        0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
        17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
    ];
    assert_eq!(a, expected);

    // Single index checks
    for x in 0..dims[0] {
        for y in 0..dims[1] {
            let value = read_var.read(&[x..x + 1, y..y + 1], 65536, 512);
            assert_eq!(value, vec![(x * 5 + y) as f32]);
        }
    }

    // Read into an existing array with an offset
    for x in 0..dims[0] {
        for y in 0..dims[1] {
            let mut r = vec![f32::NAN; 9];
            // Assuming `read_into` method exists and has the following signature:
            // fn read_into(&self, into: &mut [f32], dim_read: &[Range<usize>], into_cube_offset: &[usize], into_cube_dimension: &[usize], io_size_max: usize, io_size_merge: usize)
            read_var.read_into(&mut r, &[x..x + 1, y..y + 1], &[1, 1], &[3, 3], 65536, 512);
            let expected = vec![
                f32::NAN,
                f32::NAN,
                f32::NAN,
                f32::NAN,
                (x * 5 + y) as f32,
                f32::NAN,
                f32::NAN,
                f32::NAN,
                f32::NAN,
            ];
            // Implement a helper function `assert_eq_with_nan` if necessary
            assert_eq_with_nan(&r, &expected, 0.001);
        }
    }

    // 2x in fast dimension
    for x in 0..dims[0] {
        for y in 0..dims[1] - 1 {
            let value = read_var.read(&[x..x + 1, y..y + 2], 65536, 512);
            assert_eq!(value, vec![(x * 5 + y) as f32, (x * 5 + y + 1) as f32]);
        }
    }

    // 2x in slow dimension
    for x in 0..dims[0] - 1 {
        for y in 0..dims[1] {
            let value = read_var.read(&[x..x + 2, y..y + 1], 65536, 512);
            assert_eq!(value, vec![(x * 5 + y) as f32, ((x + 1) * 5 + y) as f32]);
        }
    }

    // 2x2 region
    for x in 0..dims[0] - 1 {
        for y in 0..dims[1] - 1 {
            let value = read_var.read(&[x..x + 2, y..y + 2], 65536, 512);
            assert_eq!(
                value,
                vec![
                    (x * 5 + y) as f32,
                    (x * 5 + y + 1) as f32,
                    ((x + 1) * 5 + y) as f32,
                    ((x + 1) * 5 + y + 1) as f32,
                ]
            );
        }
    }

    // 3x3 region
    for x in 0..dims[0] - 2 {
        for y in 0..dims[1] - 2 {
            let value = read_var.read(&[x..x + 3, y..y + 3], 65536, 512);
            assert_eq!(
                value,
                vec![
                    (x * 5 + y) as f32,
                    (x * 5 + y + 1) as f32,
                    (x * 5 + y + 2) as f32,
                    ((x + 1) * 5 + y) as f32,
                    ((x + 1) * 5 + y + 1) as f32,
                    ((x + 1) * 5 + y + 2) as f32,
                    ((x + 2) * 5 + y) as f32,
                    ((x + 2) * 5 + y + 1) as f32,
                    ((x + 2) * 5 + y + 2) as f32,
                ]
            );
        }
    }

    // 1x5 region
    for x in 0..dims[1] {
        let value = read_var.read(&[x..x + 1, 0..5], 65536, 512);
        let expected = vec![
            (x * 5) as f32,
            (x * 5 + 1) as f32,
            (x * 5 + 2) as f32,
            (x * 5 + 3) as f32,
            (x * 5 + 4) as f32,
        ];
        assert_eq!(value, expected);
    }

    // 5x1 region
    for x in 0..dims[0] {
        let value = read_var.read(&[0..5, x..x + 1], 65536, 512);
        let expected = vec![
            x as f32,
            (x + 5) as f32,
            (x + 10) as f32,
            (x + 15) as f32,
            (x + 20) as f32,
        ];
        assert_eq!(value, expected);
    }

    // Clean up by removing the test file
    std::fs::remove_file(file)?;

    Ok(())
}

#[test]
fn test_write_v3_max_io_limit() -> Result<(), Box<dyn std::error::Error>> {
    let file = "writetest.om";
    remove_file_if_exists(file);

    // Define dimensions and writer parameters
    let dims = vec![5, 5];
    let chunk_dimensions = vec![2, 2];
    let compression = CompressionType::P4nzdec256;
    let data_type = DataType::Float;
    let scale_factor = 1.0;
    let add_offset = 0.0;
    let lut_chunk_element_count = 2;

    // Initialize the writer
    let mut writer = OmFileWriterArray::new(
        dims.clone(),
        chunk_dimensions,
        compression,
        data_type,
        scale_factor,
        add_offset,
        lut_chunk_element_count,
    );

    // Initialize the write buffer with a capacity of 1
    let mut buffer = OmWriteBuffer::new(1);

    // Create the file handle for writing
    let mut file_handle = File::create(file)?;
    let mut file_handle = file_handle.borrow_mut();

    // Define the data to write
    let data: Vec<f32> = vec![
        0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
        17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
    ];

    // Write header to the buffer
    OmFileWriter2::write_header(&mut buffer);

    // Write data to the buffer with array dimensions [5,5] and read ranges [0..5, 0..5]
    writer.write_data(&data, &[5, 5], &[0..5, 0..5], &mut file_handle, &mut buffer)?;

    // Compress LUT and retrieve metadata
    let json_variable = writer.compress_lut_and_return_meta(&mut buffer);
    let json = OmFileJSON {
        variables: vec![json_variable],
        some_attributes: None,
    };

    // Write trailer to the buffer
    OmFileWriter2::write_trailer(&mut buffer, &json)?;

    // Flush the buffer contents to the file
    buffer.write_to_file(&mut file_handle)?;

    // Open the file for reading
    let file_for_reading = File::open(file)?;
    let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;

    // Initialize the reader using the open_file method
    let reader = OmFileReader2::open_file(read_backend, lut_chunk_element_count)?;
    let variables = reader.get_variables();
    let read_var = &variables[0];

    // Read with io_size_max: 0, io_size_merge: 0
    let a = read_var.read(&[0..5, 0..5], 0, 0);
    let expected = vec![
        0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
        17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
    ];
    assert_eq!(a, expected);

    // Single index checks
    for x in 0..dims[0] {
        for y in 0..dims[1] {
            let value = read_var.read(&[x..x + 1, y..y + 1], 0, 0);
            assert_eq!(value, vec![(x * 5 + y) as f32]);
        }
    }

    // Read into an existing array with an offset
    for x in 0..dims[0] {
        for y in 0..dims[1] {
            let mut r = vec![f32::NAN; 9];
            read_var.read_into(&mut r, &[x..x + 1, y..y + 1], &[1, 1], &[3, 3], 0, 0);
            let expected = vec![
                f32::NAN,
                f32::NAN,
                f32::NAN,
                f32::NAN,
                (x * 5 + y) as f32,
                f32::NAN,
                f32::NAN,
                f32::NAN,
                f32::NAN,
            ];
            assert_eq_with_nan(&r, &expected, 0.001);
        }
    }

    // 2x in fast dimension
    for x in 0..dims[0] {
        for y in 0..dims[1] - 1 {
            let value = read_var.read(&[x..x + 1, y..y + 2], 0, 0);
            assert_eq!(value, vec![(x * 5 + y) as f32, (x * 5 + y + 1) as f32]);
        }
    }

    // 2x in slow dimension
    for x in 0..dims[0] - 1 {
        for y in 0..dims[1] {
            let value = read_var.read(&[x..x + 2, y..y + 1], 0, 0);
            assert_eq!(value, vec![(x * 5 + y) as f32, ((x + 1) * 5 + y) as f32]);
        }
    }

    // 2x2
    for x in 0..dims[0] - 1 {
        for y in 0..dims[1] - 1 {
            let value = read_var.read(&[x..x + 2, y..y + 2], 0, 0);
            assert_eq!(
                value,
                vec![
                    (x * 5 + y) as f32,
                    (x * 5 + y + 1) as f32,
                    ((x + 1) * 5 + y) as f32,
                    ((x + 1) * 5 + y + 1) as f32,
                ]
            );
        }
    }

    // 3x3
    for x in 0..dims[0] - 2 {
        for y in 0..dims[1] - 2 {
            let value = read_var.read(&[x..x + 3, y..y + 3], 0, 0);
            assert_eq!(
                value,
                vec![
                    (x * 5 + y) as f32,
                    (x * 5 + y + 1) as f32,
                    (x * 5 + y + 2) as f32,
                    ((x + 1) * 5 + y) as f32,
                    ((x + 1) * 5 + y + 1) as f32,
                    ((x + 1) * 5 + y + 2) as f32,
                    ((x + 2) * 5 + y) as f32,
                    ((x + 2) * 5 + y + 1) as f32,
                    ((x + 2) * 5 + y + 2) as f32,
                ]
            );
        }
    }

    // 1x5
    for x in 0..dims[1] {
        let value = read_var.read(&[x..x + 1, 0..5], 0, 0);
        let expected = vec![
            (x * 5) as f32,
            (x * 5 + 1) as f32,
            (x * 5 + 2) as f32,
            (x * 5 + 3) as f32,
            (x * 5 + 4) as f32,
        ];
        assert_eq!(value, expected);
    }

    // 5x1
    for x in 0..dims[0] {
        let value = read_var.read(&[0..5, x..x + 1], 0, 0);
        let expected = vec![
            x as f32,
            (x + 5) as f32,
            (x + 10) as f32,
            (x + 15) as f32,
            (x + 20) as f32,
        ];
        assert_eq!(value, expected);
    }

    // Clean up by removing the test file
    remove_file_if_exists(file);

    Ok(())
}

#[test]
fn test_old_writer_new_reader() -> Result<(), Box<dyn std::error::Error>> {
    let file = "writetest.om";
    remove_file_if_exists(file);

    // Define dimensions and writer parameters that need to be used in the reader
    let dims = vec![5, 5];
    let lut_chunk_element_count = 2;

    let result0 = Arc::new((0..10).map(|x| x as f32).collect::<Vec<f32>>());
    let result2 = Arc::new((10..20).map(|x| x as f32).collect::<Vec<f32>>());
    let result4 = Arc::new((20..25).map(|x| x as f32).collect::<Vec<f32>>());

    OmFileWriter::new(5, 5, 2, 2).write_to_file(
        file,
        CompressionType::P4nzdec256,
        1.0,
        false,
        |dim0pos| match dim0pos {
            0 => Ok(result0.as_slice()),
            2 => Ok(result2.as_slice()),
            4 => Ok(result4.as_slice()),
            _ => panic!("Not expected"),
        },
    )?;

    // Open the file for reading
    let file_for_reading = File::open(file)?;
    let read_backend = MmapFile::new(file_for_reading, Mode::ReadOnly)?;

    // Initialize the reader using the open_file method
    let reader = OmFileReader2::open_file(read_backend, lut_chunk_element_count)?;
    let variables = reader.get_variables();
    let read_var = &variables[0];

    // Read the entire data back and assert equality
    let a = read_var.read(&[0..5, 0..5], 0, 0);
    let expected = vec![
        0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
        17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
    ];
    assert_eq!(a, expected);

    // Single index checks
    for x in 0..dims[0] {
        for y in 0..dims[1] {
            let value = read_var.read(&[x..x + 1, y..y + 1], 0, 0);
            assert_eq!(value, vec![(x * 5 + y) as f32]);
        }
    }

    // Read into an existing array with an offset
    for x in 0..dims[0] {
        for y in 0..dims[1] {
            let mut r = vec![f32::NAN; 9];
            read_var.read_into(&mut r, &[x..x + 1, y..y + 1], &[1, 1], &[3, 3], 0, 0);
            let expected = vec![
                f32::NAN,
                f32::NAN,
                f32::NAN,
                f32::NAN,
                (x * 5 + y) as f32,
                f32::NAN,
                f32::NAN,
                f32::NAN,
                f32::NAN,
            ];
            assert_eq_with_nan(&r, &expected, 0.001);
        }
    }

    // 2x in fast dimension
    for x in 0..dims[0] {
        for y in 0..dims[1] - 1 {
            let value = read_var.read(&[x..x + 1, y..y + 2], 0, 0);
            assert_eq!(value, vec![(x * 5 + y) as f32, (x * 5 + y + 1) as f32]);
        }
    }

    // 2x in slow dimension
    for x in 0..dims[0] - 1 {
        for y in 0..dims[1] {
            let value = read_var.read(&[x..x + 2, y..y + 1], 0, 0);
            assert_eq!(value, vec![(x * 5 + y) as f32, ((x + 1) * 5 + y) as f32]);
        }
    }

    // 2x2 region
    for x in 0..dims[0] - 1 {
        for y in 0..dims[1] - 1 {
            let value = read_var.read(&[x..x + 2, y..y + 2], 0, 0);
            assert_eq!(
                value,
                vec![
                    (x * 5 + y) as f32,
                    (x * 5 + y + 1) as f32,
                    ((x + 1) * 5 + y) as f32,
                    ((x + 1) * 5 + y + 1) as f32,
                ]
            );
        }
    }

    // 3x3 region
    for x in 0..dims[0] - 2 {
        for y in 0..dims[1] - 2 {
            let value = read_var.read(&[x..x + 3, y..y + 3], 0, 0);
            assert_eq!(
                value,
                vec![
                    (x * 5 + y) as f32,
                    (x * 5 + y + 1) as f32,
                    (x * 5 + y + 2) as f32,
                    ((x + 1) * 5 + y) as f32,
                    ((x + 1) * 5 + y + 1) as f32,
                    ((x + 1) * 5 + y + 2) as f32,
                    ((x + 2) * 5 + y) as f32,
                    ((x + 2) * 5 + y + 1) as f32,
                    ((x + 2) * 5 + y + 2) as f32,
                ]
            );
        }
    }

    // 1x5 region
    for x in 0..dims[1] {
        let value = read_var.read(&[x..x + 1, 0..5], 0, 0);
        let expected = vec![
            (x * 5) as f32,
            (x * 5 + 1) as f32,
            (x * 5 + 2) as f32,
            (x * 5 + 3) as f32,
            (x * 5 + 4) as f32,
        ];
        assert_eq!(value, expected);
    }

    // 5x1 region
    for x in 0..dims[0] {
        let value = read_var.read(&[0..5, x..x + 1], 0, 0);
        let expected = vec![
            x as f32,
            (x + 5) as f32,
            (x + 10) as f32,
            (x + 15) as f32,
            (x + 20) as f32,
        ];
        assert_eq!(value, expected);
    }

    // Clean up by removing the test file
    remove_file_if_exists(file);

    Ok(())
}

#[test]
fn test_nan() -> Result<(), Box<dyn std::error::Error>> {
    let file = "writetest_nan.om";
    remove_file_if_exists(file);

    let data: Vec<f32> = (0..(5 * 5)).map(|_| f32::NAN).collect();

    OmFileWriter::new(5, 5, 5, 5).write_to_file(
        file,
        CompressionType::P4nzdec256,
        1.0,
        false,
        |_| Ok(data.as_slice()),
    )?;

    let reader = OmFileReader::from_file(file)?;

    // assert that all values are nan
    assert!(reader
        .read_range(Some(1..2), Some(1..2))?
        .iter()
        .all(|x| x.is_nan()));
    remove_file_if_exists(file);
    Ok(())
}

#[test]
fn test_write() -> Result<(), OmFilesRsError> {
    let file = "writetest.om";
    remove_file_if_exists(file);

    let result0 = Arc::new((0..10).map(|x| x as f32).collect::<Vec<f32>>());
    let result2 = Arc::new((10..20).map(|x| x as f32).collect::<Vec<f32>>());
    let result4 = Arc::new((20..25).map(|x| x as f32).collect::<Vec<f32>>());

    OmFileWriter::new(5, 5, 2, 2).write_to_file(
        file,
        CompressionType::P4nzdec256,
        1.0,
        false,
        |dim0pos| match dim0pos {
            0 => Ok(result0.as_slice()),
            2 => Ok(result2.as_slice()),
            4 => Ok(result4.as_slice()),
            _ => panic!("Not expected"),
        },
    )?;

    let read = OmFileReader::from_file(file)?;
    let a = read.read_range(Some(0..5), Some(0..5))?;
    assert_eq!(
        a,
        vec![
            0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0,
            16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0
        ]
    );

    // single index
    for x in 0..read.dimensions.dim0 {
        for y in 0..read.dimensions.dim1 {
            assert_eq!(
                read.read_range(Some(x..x + 1), Some(y..y + 1))?,
                vec![x as f32 * 5.0 + y as f32]
            );
        }
    }

    // 2x in fast dim
    for x in 0..read.dimensions.dim0 {
        for y in 0..read.dimensions.dim1 - 1 {
            assert_eq!(
                read.read_range(Some(x..x + 1), Some(y..y + 2))?,
                vec![x as f32 * 5.0 + y as f32, x as f32 * 5.0 + y as f32 + 1.0]
            );
        }
    }

    // 2x in slow dim
    for x in 0..read.dimensions.dim0 - 1 {
        for y in 0..read.dimensions.dim1 {
            assert_eq!(
                read.read_range(Some(x..x + 2), Some(y..y + 1))?,
                vec![x as f32 * 5.0 + y as f32, (x as f32 + 1.0) * 5.0 + y as f32]
            );
        }
    }

    // 2x2
    for x in 0..read.dimensions.dim0 - 1 {
        for y in 0..read.dimensions.dim1 - 1 {
            assert_eq!(
                read.read_range(Some(x..x + 2), Some(y..y + 2))?,
                vec![
                    x as f32 * 5.0 + y as f32,
                    x as f32 * 5.0 + y as f32 + 1.0,
                    (x as f32 + 1.0) * 5.0 + y as f32,
                    (x as f32 + 1.0) * 5.0 + y as f32 + 1.0
                ]
            );
        }
    }

    // 3x3
    for x in 0..read.dimensions.dim0 - 2 {
        for y in 0..read.dimensions.dim1 - 2 {
            assert_eq!(
                read.read_range(Some(x..x + 3), Some(y..y + 3))?,
                vec![
                    x as f32 * 5.0 + y as f32,
                    x as f32 * 5.0 + y as f32 + 1.0,
                    x as f32 * 5.0 + y as f32 + 2.0,
                    (x as f32 + 1.0) * 5.0 + y as f32,
                    (x as f32 + 1.0) * 5.0 + y as f32 + 1.0,
                    (x as f32 + 1.0) * 5.0 + y as f32 + 2.0,
                    (x as f32 + 2.0) * 5.0 + y as f32,
                    (x as f32 + 2.0) * 5.0 + y as f32 + 1.0,
                    (x as f32 + 2.0) * 5.0 + y as f32 + 2.0
                ]
            );
        }
    }

    // 1x5
    for x in 0..read.dimensions.dim1 {
        assert_eq!(
            read.read_range(Some(x..x + 1), Some(0..5))?,
            vec![
                x as f32 * 5.0,
                x as f32 * 5.0 + 1.0,
                x as f32 * 5.0 + 2.0,
                x as f32 * 5.0 + 3.0,
                x as f32 * 5.0 + 4.0
            ]
        );
    }

    // 5x1
    for x in 0..read.dimensions.dim0 {
        assert_eq!(
            read.read_range(Some(0..5), Some(x..x + 1))?,
            vec![
                x as f32,
                x as f32 + 5.0,
                x as f32 + 10.0,
                x as f32 + 15.0,
                x as f32 + 20.0
            ]
        );
    }

    // // test interpolation
    // assert_eq!(
    //     read.read_interpolated(0, 0.5, 0, 0.5, 2, 0..5)?,
    //     vec![7.5, 8.5, 9.5, 10.5, 11.5]
    // );
    // assert_eq!(
    //     read.read_interpolated(0, 0.1, 0, 0.2, 2, 0..5)?,
    //     vec![2.5, 3.4999998, 4.5, 5.5, 6.5]
    // );
    // assert_eq!(
    //     read.read_interpolated(0, 0.9, 0, 0.2, 2, 0..5)?,
    //     vec![6.5, 7.5, 8.5, 9.5, 10.5]
    // );
    // assert_eq!(
    //     read.read_interpolated(0, 0.1, 0, 0.9, 2, 0..5)?,
    //     vec![9.5, 10.499999, 11.499999, 12.5, 13.499999]
    // );
    // assert_eq!(
    //     read.read_interpolated(0, 0.8, 0, 0.9, 2, 0..5)?,
    //     vec![12.999999, 14.0, 15.0, 16.0, 17.0]
    // );

    Ok(())
}

#[test]
fn test_write_fpx() -> Result<(), Box<dyn std::error::Error>> {
    let file = "writetest_fpx.om";
    remove_file_if_exists(file);

    let result0 = Arc::new((0..10).map(|x| x as f32).collect::<Vec<f32>>());
    let result2 = Arc::new((10..20).map(|x| x as f32).collect::<Vec<f32>>());
    let result4 = Arc::new((20..25).map(|x| x as f32).collect::<Vec<f32>>());

    OmFileWriter::new(5, 5, 2, 2).write_to_file(
        file,
        CompressionType::Fpxdec32,
        1.0,
        false,
        |dim0pos| match dim0pos {
            0 => Ok(result0.as_slice()),
            2 => Ok(result2.as_slice()),
            4 => Ok(result4.as_slice()),
            _ => panic!("Not expected"),
        },
    )?;

    let reader = OmFileReader::from_file(file)?;
    let a = reader.read_range(Some(0..5), Some(0..5))?;
    assert_eq!(
        a,
        vec![
            0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0,
            16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0
        ]
    );

    // single index
    for x in 0..reader.dimensions.dim0 {
        for y in 0..reader.dimensions.dim1 {
            assert_eq!(
                reader.read_range(Some(x..x + 1), Some(y..y + 1))?,
                vec![x as f32 * 5.0 + y as f32]
            );
        }
    }

    // 2x in fast dim
    for x in 0..reader.dimensions.dim0 {
        for y in 0..reader.dimensions.dim1 - 1 {
            assert_eq!(
                reader.read_range(Some(x..x + 1), Some(y..y + 2))?,
                vec![x as f32 * 5.0 + y as f32, x as f32 * 5.0 + y as f32 + 1.0]
            );
        }
    }

    // 2x in slow dim
    for x in 0..reader.dimensions.dim0 - 1 {
        for y in 0..reader.dimensions.dim1 {
            assert_eq!(
                reader.read_range(Some(x..x + 2), Some(y..y + 1))?,
                vec![x as f32 * 5.0 + y as f32, (x as f32 + 1.0) * 5.0 + y as f32]
            );
        }
    }

    // 2x2
    for x in 0..reader.dimensions.dim0 - 1 {
        for y in 0..reader.dimensions.dim1 - 1 {
            assert_eq!(
                reader.read_range(Some(x..x + 2), Some(y..y + 2))?,
                vec![
                    x as f32 * 5.0 + y as f32,
                    x as f32 * 5.0 + y as f32 + 1.0,
                    (x as f32 + 1.0) * 5.0 + y as f32,
                    (x as f32 + 1.0) * 5.0 + y as f32 + 1.0
                ]
            );
        }
    }

    // 3x3
    for x in 0..reader.dimensions.dim0 - 2 {
        for y in 0..reader.dimensions.dim1 - 2 {
            assert_eq!(
                reader.read_range(Some(x..x + 3), Some(y..y + 3))?,
                vec![
                    x as f32 * 5.0 + y as f32,
                    x as f32 * 5.0 + y as f32 + 1.0,
                    x as f32 * 5.0 + y as f32 + 2.0,
                    (x as f32 + 1.0) * 5.0 + y as f32,
                    (x as f32 + 1.0) * 5.0 + y as f32 + 1.0,
                    (x as f32 + 1.0) * 5.0 + y as f32 + 2.0,
                    (x as f32 + 2.0) * 5.0 + y as f32,
                    (x as f32 + 2.0) * 5.0 + y as f32 + 1.0,
                    (x as f32 + 2.0) * 5.0 + y as f32 + 2.0
                ]
            );
        }
    }

    // 1x5
    for x in 0..reader.dimensions.dim1 {
        assert_eq!(
            reader.read_range(Some(x..x + 1), Some(0..5))?,
            vec![
                x as f32 * 5.0,
                x as f32 * 5.0 + 1.0,
                x as f32 * 5.0 + 2.0,
                x as f32 * 5.0 + 3.0,
                x as f32 * 5.0 + 4.0
            ]
        );
    }

    // 5x1
    for x in 0..reader.dimensions.dim0 {
        assert_eq!(
            reader.read_range(Some(0..5), Some(x..x + 1))?,
            vec![
                x as f32,
                x as f32 + 5.0,
                x as f32 + 10.0,
                x as f32 + 15.0,
                x as f32 + 20.0
            ]
        );
    }

    remove_file_if_exists(file);

    Ok(())
}

fn assert_eq_with_accuracy(expected: &[f32], actual: &[f32], accuracy: f32) {
    assert_eq!(expected.len(), actual.len());
    for (e, a) in expected.iter().zip(actual.iter()) {
        assert!((e - a).abs() < accuracy, "Expected: {}, Actual: {}", e, a);
    }
}

// Helper function to assert equality with NaN handling and a specified accuracy
fn assert_eq_with_nan(actual: &[f32], expected: &[f32], accuracy: f32) {
    assert_eq!(actual.len(), expected.len(), "Lengths differ");
    for (a, e) in actual.iter().zip(expected.iter()) {
        if e.is_nan() {
            assert!(a.is_nan(), "Expected NaN, found {}", a);
        } else {
            assert!(
                (a - e).abs() <= accuracy,
                "Values differ: expected {}, found {}",
                e,
                a
            );
        }
    }
}

fn remove_file_if_exists(file: &str) {
    if fs::metadata(file).is_ok() {
        fs::remove_file(file).unwrap();
    }
}