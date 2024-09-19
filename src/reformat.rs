use omfiles_rs::compression::CompressionType;
use omfiles_rs::om::reader::OmFileReader;
use omfiles_rs::om::writer::OmFileWriter;
// use std::env;
use std::{
    io::{self},
    rc::Rc,
};

fn main() -> io::Result<()> {
    let control_range_dim0 = Some(10000..10001);
    let control_range_dim1 = Some(0..100);
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
    // let input_file_path = "icond2_temp2m_chunk_3960.om";
    // let output_file_path = "icond2_test_pico.om";

    // Read data from the input OM file
    let reader = OmFileReader::from_file(input_file_path)
        .expect(format!("Failed to open file: {}", input_file_path).as_str());
    println!("compression: {:?}", reader.compression);
    println!("dim0: {:}", reader.dimensions.dim0);
    println!("dim1: {:}", reader.dimensions.dim1);
    println!("chunk0: {:}", reader.dimensions.chunk0);
    println!("chunk1: {:}", reader.dimensions.chunk1);
    println!("scalefactor: {:}", reader.scalefactor);

    // Write the compressed data to the output OM file
    let writer = OmFileWriter::new(
        reader.dimensions.dim0,
        reader.dimensions.dim1,
        reader.dimensions.chunk0,
        reader.dimensions.chunk1,
    );

    let control_data_original = reader
        .read_range(control_range_dim0.clone(), control_range_dim1.clone())
        .expect("Failed to read defined data ranges");

    writer
        .write_to_file(
            &output_file_path,
            CompressionType::Pico,
            reader.scalefactor,
            true,
            move |dim0pos| {
                let dim0_end = std::cmp::min(writer.dim0, dim0pos + writer.chunk0);
                let blub = reader
                    .read_range(Some(dim0pos..dim0_end), None)
                    .expect("Failed to read data");

                let reference: Rc<Vec<f32>> = Rc::from(blub);

                Ok(reference.clone())
            },
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
