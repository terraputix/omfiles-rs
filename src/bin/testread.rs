use omfiles_rs::io::reader::OmFileReader;
use std::env;
use std::io::{self};
use std::ops::Range;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} <file_path> <dim0_range> <dim1_range>", args[0]);
        eprintln!("Example: {} omfile.om 1..2 0..104", args[0]);
        return Ok(());
    }

    let file_path = &args[1];
    let dim0_range = parse_range(&args[2]);
    let dim1_range = parse_range(&args[3]);

    let reader = OmFileReader::from_file(file_path)
        .expect(format!("Failed to open file: {}", file_path).as_str());
    println!("compression: {:?}", reader.compression);
    println!("dim0: {:}", reader.dimensions.dim0);
    println!("dim1: {:}", reader.dimensions.dim1);
    println!("chunk0: {:}", reader.dimensions.chunk0);
    println!("chunk1: {:}", reader.dimensions.chunk1);

    let data = reader
        .read_range(dim0_range, dim1_range)
        .expect("Failed to read defined data ranges");

    println!("{:?}", data);
    Ok(())
}

fn parse_range(range_str: &str) -> Option<Range<usize>> {
    let parts: Vec<&str> = range_str.split("..").collect();
    if parts.len() != 2 {
        return None;
    }
    let start = parts[0].parse::<usize>().ok()?;
    let end = parts[1].parse::<usize>().ok()?;
    Some(start..end)
}
