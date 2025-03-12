use ndarray::Array3;
use omfiles_rs::core::compression::CompressionType;
use omfiles_rs::io::reader::OmFileReader;
use omfiles_rs::io::writer::OmFileWriter;
use std::fs::File;
use std::io;

fn main() -> io::Result<()> {
    let control_range_dim0 = 10000..10001;
    let control_range_dim1 = 0..100;
    let input_file_path = "chunk_1910.om";
    let output_file_path = "chunk_1910_reformatted.om";

    // Read data from the input OM file
    let reader = OmFileReader::from_file(input_file_path)
        .expect(format!("Failed to open file: {}", input_file_path).as_str());

    let dimensions = reader.get_dimensions();
    let chunks = reader.get_chunk_dimensions();

    println!("Input file info:");
    println!("compression: {:?}", reader.compression());
    println!("dimensions: {:?}", dimensions);
    println!("chunks: {:?}", chunks);
    println!("scale_factor: {}", reader.scale_factor());

    // Original dimensions are [lat, lon, time]
    let lat_dim = dimensions[0];
    let lon_dim = dimensions[1];
    let time_dim = dimensions[2];

    let file_handle = File::create(output_file_path).expect("Failed to create output file");
    // Write the compressed data to the output OM file
    let mut file_writer = OmFileWriter::new(
        &file_handle,
        1024 * 1024 * 1024, // Initial capacity of 10MB
    );
    println!("created writer");

    let reformatted_dimensions = vec![time_dim, lat_dim, lon_dim];
    // Choose appropriate chunk dimensions for the new layout
    // A sensible default might be to use single time slices as chunks
    let rechunked_dimensions = vec![1, lat_dim, lon_dim];

    let mut writer = file_writer
        .prepare_array::<f32>(
            reformatted_dimensions.clone(),
            rechunked_dimensions.clone(),
            CompressionType::PforDelta2dInt16,
            reader.scale_factor(),
            reader.add_offset(),
        )
        .expect("Failed to prepare array");

    println!("Prepared output array");
    println!("Reformatting data from [lat, lon, time] to [time, lat, lon]...");

    // Process one time slice at a time
    for t in 0..time_dim {
        // Read a time slice from the input file
        let time_slice_data = reader
            .read(&[0u64..lat_dim, 0u64..lon_dim, t..t + 1u64], None, None)
            .expect("Failed to read data");

        // Reshape the data from [lat, lon, 1] to [1, lat, lon]
        // First reshape into a 3D array
        let reshaped_data = Array3::<f32>::from_shape_vec(
            (lat_dim as usize, lon_dim as usize, 1),
            time_slice_data.into_raw_vec(),
        )
        .expect("Failed to reshape data");

        // Transpose from [lat, lon, 1] to [1, lat, lon]
        let transposed = reshaped_data.permuted_axes([2, 0, 1]);

        // Write this time slice to the new file
        // The output layout is [time, lat, lon]
        writer
            .write_data(transposed.into_dyn().view(), None, None)
            .expect(&format!("Failed to write data for time {}", t));

        if t % 10 == 0 || t == time_dim - 1 {
            println!("Processed time slice {}/{}", t + 1, time_dim);
        }
    }

    let variable_meta = writer.finalize();
    println!("Finalized Array");

    let variable = file_writer
        .write_array(variable_meta, "data", &[])
        .expect("Failed to write array metadata");
    file_writer
        .write_trailer(variable)
        .expect("Failed to write trailer");

    println!("Finished writing");

    // Verify the output
    let reader = OmFileReader::from_file(output_file_path)
        .expect(format!("Failed to open file: {}", output_file_path).as_str());

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
