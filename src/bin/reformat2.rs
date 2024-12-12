use omfiles_rs::core::compression::CompressionType;
use omfiles_rs::io::reader2::OmFileReader2;
use omfiles_rs::io::writer2::OmFileWriter2;
use std::fs::File;
use std::io;

fn main() -> io::Result<()> {
    let control_range_dim0 = 10000..10001;
    let control_range_dim1 = 0..100;
    let input_file_path = "icond2_test_reformatted.om";
    let output_file_path = "icond2_test_reformatted_v2.om";

    // Read data from the input OM file
    let reader = OmFileReader2::from_file(input_file_path)
        .expect(format!("Failed to open file: {}", input_file_path).as_str());

    let dimensions = reader.get_dimensions();
    let chunks = reader.get_chunk_dimensions();

    println!("compression: {:?}", reader.compression());
    println!("dimensions: {:?}", dimensions);
    println!("chunks: {:?}", chunks);
    println!("scale_factor: {}", reader.scale_factor());

    let control_data_original = reader
        .read_simple(
            &[control_range_dim0.clone(), control_range_dim1.clone()],
            None,
            None,
        )
        .expect("Failed to read defined data ranges");

    let file_handle = File::create(output_file_path).expect("Failed to create output file");
    // Write the compressed data to the output OM file
    let mut file_writer = OmFileWriter2::new(
        &file_handle,
        1024 * 1024 * 10, // Initial capacity of 10MB
    );
    println!("created writer");

    // let rechunked_dimensions = vec![50, 121];
    // let rechunked_dimensions = chunks.iter().map(|&x| x).collect::<Vec<_>>();
    // let rechunked_dimensions = vec![chunks[0] * 200, chunks[1]];
    // let rechunked_dimensions = vec![chunks[0], chunks[1]];
    let rechunked_dimensions = vec![dimensions[0], dimensions[1]];
    println!("rechunked_dimensions: {:?}", &rechunked_dimensions);

    let mut writer = file_writer
        .prepare_array::<f32>(
            dimensions.to_vec(),
            rechunked_dimensions.clone(),
            CompressionType::P4nzdec256,
            reader.scale_factor(),
            reader.add_offset(),
            256, // lut_chunk_element_count
        )
        .expect("Failed to prepare array");

    println!("prepared array");

    // Read and write data in chunks
    // Iterate over both chunk dimensions at once
    for chunk_start in (0..dimensions[0]).step_by(rechunked_dimensions[0] as usize) {
        let chunk_dim_0 = std::cmp::min(rechunked_dimensions[0], dimensions[0] - chunk_start);

        let chunk_data = reader
            .read_simple(
                &[chunk_start..chunk_start + chunk_dim_0, 0..dimensions[1]],
                None,
                None,
            )
            .expect("Failed to read chunk data");

        writer
            .write_data(
                chunk_data.as_slice(),
                Some(&[chunk_dim_0, dimensions[1]]),
                None,
                None,
            )
            .expect("Failed to write chunk data");
    }

    let variable_meta = writer.finalize();
    println!("Finalized Array");

    let variable = file_writer
        .write_array(variable_meta, "data", &[])
        .expect("Failed to write array metadata");
    file_writer
        .write_trailer(variable)
        .expect("Failed to write trailer");

    // let array_offset = writer
    //     .write_array(finalized_array, "data", &[])
    //     .expect("Failed to write array metadata");

    // file_writer
    //     .write_trailer(array_offset)
    //     .expect("Failed to write trailer");

    println!("Finished writing");

    // Verify the output
    let reader = OmFileReader2::from_file(output_file_path)
        .expect(format!("Failed to open file: {}", output_file_path).as_str());

    let control_data_new = reader
        .read_simple(
            &[control_range_dim0.clone(), control_range_dim1.clone()],
            None,
            None,
        )
        .expect("Failed to read defined data ranges");

    println!("data from newly written file: {:?}", control_data_new);
    assert_eq!(control_data_original, control_data_new, "Data mismatch");

    Ok(())
}
