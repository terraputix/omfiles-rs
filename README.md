# omfiles-rs

Reader and Writer implementation for the `om` file format.

The file format is documented in the [open-meteo/om-file-format](https://github.com/open-meteo/om-file-format/blob/main/README.md) repository.

## Development

```bash
cargo test
```

## Reading files

Assuming the file `data.om` directly contains a floating point array with 3 dimensions

```rust
use omfiles_rs::io::reader::OmFileReader;

let file = "data.om";
let reader = OmFileReader::from_file(file).expect(format!("Failed to open file: {}", file).as_str());
// read root variable into float array
let data = reader.read::<f32>(&[50u64..51, 20..21, 10..200], None, None).expect("Failed to read data");

// Metadata information
let dimensions = reader.get_dimensions();
let chunk_size = reader.get_chunk_dimensions();
println!("Dimensions: {:?}", dimensions);
println!("Chunk size: {:?}", chunk_size);
```

```bash
cargo run --bin testread era5land_temp2m_chunk_951.om 1..2 0..104
```

Feel free to adapt the code in `src/testread.rs` to your needs.
