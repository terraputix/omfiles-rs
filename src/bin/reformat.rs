use omfiles_rs::core::compression::CompressionType;
use omfiles_rs::io::reader::OmFileReader;
use omfiles_rs::io::writer::OmFileWriter;
use std::{
    io::{self},
    rc::Rc,
};

fn main() -> io::Result<()> {
    let control_range_dim0 = Some(10000..10001);
    let control_range_dim1 = Some(0..100);
    let input_file_path = "icond2_temp2m_chunk_3960.om";
    let output_file_path = "icond2_test_reformatted.om";

    // Read data from the input OM file
    let reader = OmFileReader::from_file(input_file_path)
        .expect(format!("Failed to open file: {}", input_file_path).as_str());
    println!("compression: {:?}", reader.compression);
    println!("dim0: {:}", reader.dimensions.dim0);
    println!("dim1: {:}", reader.dimensions.dim1);
    println!("chunk0: {:}", reader.dimensions.chunk0);
    println!("chunk1: {:}", reader.dimensions.chunk1);
    println!("scalefactor: {:}", reader.scale_factor);

    let control_data_original = reader
        .read_range(control_range_dim0.clone(), control_range_dim1.clone())
        .expect("Failed to read defined data ranges");

    // Write the compressed data to the output OM file
    let writer = OmFileWriter::new(
        reader.dimensions.dim0,
        reader.dimensions.dim1,
        reader.dimensions.chunk0,
        reader.dimensions.chunk1,
    );

    let supply_chunk = |chunk_start_pos| {
        let chunk_end_pos = std::cmp::min(writer.dim0, chunk_start_pos + writer.chunk0);
        let chunk_data = reader
            .read_range(Some(chunk_start_pos..chunk_end_pos), None)
            .expect("Failed to read data");

        let shared_chunk_data: Rc<Vec<f32>> = Rc::from(chunk_data);

        Ok(shared_chunk_data.clone())
    };

    writer
        .write_to_file(
            &output_file_path,
            CompressionType::P4nzdec256logarithmic,
            2000.0,
            true,
            supply_chunk,
        )
        .expect("Failed to write data to output file");

    // read some data from the file to verify the output
    let reader = OmFileReader::from_file(output_file_path)
        .expect(format!("Failed to open file: {}", output_file_path).as_str());

    let control_data_pico = reader
        .read_range(control_range_dim0, control_range_dim1)
        .expect("Failed to read defined data ranges");

    println!("data from newly written file: {:?}", control_data_pico);
    assert_eq!(control_data_original, control_data_pico, "Data mismatch");

    Ok(())
}
