use omfiles_rs::backend::mmapfile::MmapFile;
use omfiles_rs::errors::OmFilesRsError;
use omfiles_rs::io::reader::OmFileReader;
use std::env;

/// Display information about a variable and its children recursively
fn print_variable_info(reader: &OmFileReader<MmapFile>, indent: usize, path: &str) {
    let indent_str = " ".repeat(indent);

    // Get variable name
    let name = reader.get_name().unwrap_or_else(|| "unnamed".to_string());

    // Get variable type
    let data_type = reader.data_type();

    // Get compression type
    let compression = reader.compression();

    // Get dimensions
    let dimensions = reader.get_dimensions();
    let dims_str = dimensions
        .iter()
        .map(|d| d.to_string())
        .collect::<Vec<_>>()
        .join(" × ");

    // Get chunks
    let chunks = reader.get_chunk_dimensions();
    let chunks_str = if !chunks.is_empty() {
        chunks
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(" × ")
    } else {
        "none".to_string()
    };

    // Print information
    println!("{}Variable: {}", indent_str, path);
    println!("{}  Name: {}", indent_str, name);
    println!("{}  Type: {:?}", indent_str, data_type);
    println!("{}  Compression: {:?}", indent_str, compression);
    println!("{}  Dimensions: [{}]", indent_str, dims_str);
    println!("{}  Chunks: [{}]", indent_str, chunks_str);

    // Process children recursively
    let num_children = reader.number_of_children();
    for i in 0..num_children {
        if let Some(child) = reader.get_child(i) {
            let child_name = child.get_name().unwrap_or_else(|| format!("child_{}", i));
            let child_path = if path.is_empty() {
                child_name.clone()
            } else {
                format!("{}/{}", path, child_name)
            };
            print_variable_info(&child, indent + 2, &child_path);
        }
    }
}

fn main() -> Result<(), OmFilesRsError> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <om-file>", args[0]);
        std::process::exit(1);
    }

    let filename = &args[1];

    // Open the OM file
    let reader = OmFileReader::from_file(filename)?;

    println!("OM File: {}", filename);
    println!("=========================================");

    // Start recursive traversal from the root variable
    print_variable_info(&reader, 0, "");

    Ok(())
}
