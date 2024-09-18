use omfiles_rs::om::reader::OmFileReader;
use std::io::{self};

fn main() -> io::Result<()> {
    let reader = OmFileReader::from_file("era5land_temp2m_chunk_951.om").unwrap();
    println!("compression: {:?}", reader.compression);

    let data = reader.read_range(Some(1..2), Some(0..104)).unwrap();

    println!("{:?}", data);
    Ok(())
}
