use omfiles_rs::compression::CompressionType;
use omfiles_rs::om::reader::OmFileReader;
use omfiles_rs::om::writer::OmFileWriter;
// use std::env;
use std::io::{self};

fn main() -> io::Result<()> {
    // let args: Vec<String> = env::args().collect();
    // if args.len() != 3 {
    //     eprintln!(
    //         "Usage: {} <input_file_path> <output_file_path> <dim0_range> <dim1_range>",
    //         args[0]
    //     );
    //     return Ok(());
    // }

    // let input_file_path = &args[1];
    // let output_file_path = &args[2];
    let input_file_path = "era5land_temp2m_chunk_951.om";
    let output_file_path = "era5land_test_pico.om";

    // Read data from the input OM file
    let reader = OmFileReader::from_file(input_file_path)
        .expect(format!("Failed to open file: {}", input_file_path).as_str());
    println!("compression: {:?}", reader.compression);
    println!("dim0: {:}", reader.dimensions.dim0);
    println!("dim1: {:}", reader.dimensions.dim1);
    println!("chunk0: {:}", reader.dimensions.chunk0);
    println!("chunk1: {:}", reader.dimensions.chunk1);

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
            CompressionType::Pico,
            reader.scalefactor,
            &data,
            false,
        )
        .expect("Failed to write data to output file");

    Ok(())
}
