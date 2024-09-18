# omfiles-rs

WIP reader and writer implementation for the `om` file format.

The file format is documented in the [open-meteo/open-data](https://github.com/open-meteo/open-data/blob/0ddd57363cee1f7664197b915aaad2d15007b079/README.md#file-format) repository and in the [open-meteo source code](https://github.com/open-meteo/open-meteo/blob/3e4c22c0b61919752c7a53d0e60ecb2a86b94f41/Sources/SwiftPFor2D/SwiftPFor2D.swift).

This code depends on [turbo-pfor](https://github.com/powturbo/TurboPFor-Integer-Compression), which is a C library for fast integer compression.
There are custom bindings to this code in [turbopfor-om-rs](https://github.com/terraputix/turbopfor-om-rs) (currently not published on crates.io).

## Development

```bash
cargo test
```

## Reading OM Files

This project provides a `testread` binary that can be used to read OM files:

```bash
cargo run --bin testread era5land_temp2m_chunk_951.om 1..2 0..104
```

Feel free to adapt the code in `src/testread.rs` to your needs.
