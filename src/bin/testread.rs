use omfiles_rs::{
    backend::mmapfile::{MmapFile, Mode},
    io::reader::OmFileReader,
};
use std::{env, fs::File, io, ops::Range, sync::Arc};

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: {} <file_path> <dim0_range> [<dim1_range> ...]",
            args[0]
        );
        eprintln!("Example: {} omfile.om 1..2 0..104 0..50", args[0]);
        return Ok(());
    }

    let file_path = &args[1];
    // Parse all ranges after the file path
    let ranges: Vec<Option<Range<u64>>> = args[2..].iter().map(|s| parse_range(s)).collect();

    // Open the file and create the reader with the new structure
    let file = File::open(file_path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to open file: {}", e)))?;
    let backend = MmapFile::new(file, Mode::ReadOnly).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to create backend: {}", e),
        )
    })?;
    let reader = OmFileReader::new(Arc::new(backend)).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to create reader: {}", e),
        )
    })?;

    // Get dimensions from the new reader structure
    let dims = reader.get_dimensions();
    let chunk_dims = reader.get_chunk_dimensions();
    println!("dimensions: {:?}", dims);
    println!("chunk_dimensions: {:?}", chunk_dims);

    // Verify that the number of ranges matches the number of dimensions
    if ranges.len() != dims.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Number of ranges ({}) doesn't match number of dimensions ({})",
                ranges.len(),
                dims.len()
            ),
        ));
    }

    // Convert all Option<Range> to actual ranges
    if ranges.iter().all(|r| r.is_some()) {
        let ranges: Vec<Range<u64>> = ranges.into_iter().map(|r| r.unwrap()).collect();

        let data = reader.read::<f32>(&ranges, None, None).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Failed to read data: {}", e))
        })?;

        println!("{:?}", data);
    } else {
        eprintln!("Invalid range format in one or more dimensions");
    }

    Ok(())
}

fn parse_range(range_str: &str) -> Option<Range<u64>> {
    let parts: Vec<&str> = range_str.split("..").collect();
    if parts.len() != 2 {
        return None;
    }
    let start = parts[0].parse::<u64>().ok()?;
    let end = parts[1].parse::<u64>().ok()?;
    Some(start..end)
}
