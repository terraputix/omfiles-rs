use omfiles_rs::compression::CompressionType;
use omfiles_rs::om::reader::OmFileReader;
use omfiles_rs::om::writer::OmFileWriter;
use std::io::{self};

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

    // read all data
    let data = reader
        .read_range(None, None)
        .expect("Failed to read all data from the file");

    // Write the compressed data to the output OM file
    let writer = OmFileWriter::new(
        reader.dimensions.dim0,
        reader.dimensions.dim1,
        reader.dimensions.chunk0,
        reader.dimensions.chunk1,
    );

    writer
        .write_all_to_file(
            &output_file_path,
            CompressionType::P4nzdec256logarithmic,
            2000.0,
            &data,
            true,
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
