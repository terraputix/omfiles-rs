use ndarray::Array3;
use omfiles_rs::core::compression::CompressionType;
use omfiles_rs::io::reader::OmFileReader;
use omfiles_rs::io::writer::OmFileWriter;
use std::fs::File;
use std::io;

fn main() -> io::Result<()> {
    let u_wind_file_path = "uwind_icon_chunk1914.om";
    let v_wind_file_path = "vwind_icon_chunk1914.om";
    let output_file_path = "wind_speed_icon_chunk1914.om";

    // Read data from the u wind file
    let u_reader = OmFileReader::from_file(u_wind_file_path)
        .expect(format!("Failed to open file: {}", u_wind_file_path).as_str());

    // Read data from the v wind file
    let v_reader = OmFileReader::from_file(v_wind_file_path)
        .expect(format!("Failed to open file: {}", v_wind_file_path).as_str());

    // Check that the dimensions match between files
    let u_dimensions = u_reader.get_dimensions();
    let v_dimensions = v_reader.get_dimensions();

    if u_dimensions != v_dimensions {
        panic!("Dimensions of u and v wind files do not match");
    }

    println!("Input file info:");
    println!("u wind file - dimensions: {:?}", u_dimensions);
    println!("u wind file - compression: {:?}", u_reader.compression());
    println!("u wind file - scale_factor: {}", u_reader.scale_factor());

    // Original dimensions are [lat, lon, time]
    let lat_dim = u_dimensions[0];
    let lon_dim = u_dimensions[1];

    // Read the first timestamp from each file
    let u_wind_data = u_reader
        .read(&[0u64..lat_dim, 0u64..lon_dim, 0..1u64], None, None)
        .expect("Failed to read u wind data");

    let v_wind_data = v_reader
        .read(&[0u64..lat_dim, 0u64..lon_dim, 0..1u64], None, None)
        .expect("Failed to read v wind data");

    // Reshape the data from each file
    let u_array = Array3::<f32>::from_shape_vec(
        (lat_dim as usize, lon_dim as usize, 1),
        u_wind_data.into_raw_vec(),
    )
    .expect("Failed to reshape u wind data");

    let v_array = Array3::<f32>::from_shape_vec(
        (lat_dim as usize, lon_dim as usize, 1),
        v_wind_data.into_raw_vec(),
    )
    .expect("Failed to reshape v wind data");

    // Calculate wind speed using Pythagorean theorem: wind_speed = sqrt(u^2 + v^2)
    let wind_speed_array = &u_array.mapv(|u| u.powi(2)) + &v_array.mapv(|v| v.powi(2));
    let wind_speed_array = wind_speed_array.mapv(|val| val.sqrt());

    // Transpose from [lat, lon, time] to [time, lat, lon]
    let wind_speed_transposed = wind_speed_array.permuted_axes([2, 0, 1]);

    // Create output file
    let file_handle = File::create(output_file_path).expect("Failed to create output file");

    // Initialize the writer
    let mut file_writer = OmFileWriter::new(
        &file_handle,
        1024 * 1024 * 1024, // Initial capacity of 1GB
    );
    println!("Created writer");

    // Define the output dimensions and chunking
    let output_dimensions = vec![1, lat_dim, lon_dim]; // [time, lat, lon]
    let output_chunks = vec![1, lat_dim, lon_dim];

    // Prepare the array for writing
    let mut writer = file_writer
        .prepare_array::<f32>(
            output_dimensions.clone(),
            output_chunks.clone(),
            CompressionType::PforDelta2d,
            u_reader.scale_factor(), // Using the same scale factor as the input
            u_reader.add_offset(),   // Using the same offset as the input
        )
        .expect("Failed to prepare array");

    println!("Prepared output array");
    println!("Writing wind speed data...");

    // Write the wind speed data
    writer
        .write_data(wind_speed_transposed.into_dyn().view(), None, None)
        .expect("Failed to write wind speed data");

    // Finalize the array
    let variable_meta = writer.finalize();
    println!("Finalized Array");

    // Write the array metadata and trailer
    let variable = file_writer
        .write_array(variable_meta, "wind_speed", &[])
        .expect("Failed to write array metadata");

    file_writer
        .write_trailer(variable)
        .expect("Failed to write trailer");

    println!("Successfully wrote wind speed data to {}", output_file_path);

    Ok(())
}
