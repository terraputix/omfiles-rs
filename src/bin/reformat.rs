use omfiles_rs::core::compression::CompressionType;
use omfiles_rs::io::reader::OmFileReader;
use omfiles_rs::io::writer::OmFileWriter;
use std::fs::File;
use std::io;

fn main() -> io::Result<()> {
    let input_file_path = "chunk_1545.om";
    let output_file_path = "chunk_1545_spatial.om";

    // Read data from the input OM file
    let reader = OmFileReader::from_file(input_file_path)
        .expect(format!("Failed to open file: {}", input_file_path).as_str());

    let dimensions = reader.get_dimensions();
    let chunks = reader.get_chunk_dimensions();

    println!("compression: {:?}", reader.compression());
    println!("dimensions: {:?}", dimensions);
    println!("chunks: {:?}", chunks);
    println!("scale_factor: {}", reader.scale_factor());

    let file_handle = File::create(output_file_path).expect("Failed to create output file");
    // Write the compressed data to the output OM file
    let mut file_writer = OmFileWriter::new(
        &file_handle,
        1024 * 1024 * 1024, // Initial capacity of 10MB
    );
    println!("created writer");

    // let rechunked_dimensions = vec![50, 121];
    // let rechunked_dimensions = chunks.iter().map(|&x| x).collect::<Vec<_>>();
    // let rechunked_dimensions = vec![chunks[0], chunks[1]];
    let reformatted_chunks = vec![1, 1, dimensions[1]];
    let reformatted_dimensions = vec![dimensions[2], dimensions[0], dimensions[1]];

    println!("reformatted_dimensions: {:?}", reformatted_dimensions);

    let mut writer = file_writer
        .prepare_array::<f32>(
            reformatted_dimensions.clone(),
            reformatted_chunks.clone(),
            CompressionType::P4nzdec256,
            reader.scale_factor(),
            reader.add_offset(),
        )
        .expect("Failed to prepare array");

    println!("prepared array");

    // Read and write data in chunks
    // Iterate over both chunk dimensions at once
    for t in (0..dimensions[2]).step_by(reformatted_chunks[0] as usize) {
        println!("t: {}", t);
        let old_read_range = vec![
            0..reformatted_dimensions[1],
            0..reformatted_dimensions[2],
            t..t + 1,
        ];
        println!("old_read_range: {:?}", old_read_range);
        // let chunk_dim_0 = std::cmp::min(rechunked_dimensions[2], dimensions[0] - chunk_start);

        let chunk_data = reader
            .read::<f32>(
                &[
                    0..reformatted_dimensions[1],
                    0..reformatted_dimensions[2],
                    t..t + 1,
                ],
                None,
                None,
            )
            .expect("Failed to read chunk data");

        writer
            .write_data(
                chunk_data.as_slice(),
                Some(&[1, reformatted_dimensions[1], reformatted_dimensions[2]]),
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
    // let reader = OmFileReader::from_file(output_file_path)
    //     .expect(format!("Failed to open file: {}", output_file_path).as_str());

    // let control_data_new = reader
    //     .read::<f32>(
    //         &[control_range_dim0.clone(), control_range_dim1.clone()],
    //         None,
    //         None,
    //     )
    //     .expect("Failed to read defined data ranges");

    // println!("data from newly written file: {:?}", control_data_new);
    // assert_eq!(control_data_original, control_data_new, "Data mismatch");

    Ok(())
}
