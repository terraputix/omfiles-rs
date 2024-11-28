use criterion::{black_box, criterion_group, criterion_main, Criterion};
use omfiles_rs::compression::CompressionType;
use omfiles_rs::om::reader::OmFileReader;
use omfiles_rs::om::writer::OmFileWriter;
use rand::Rng;
use std::fs;
use std::rc::Rc;
use std::time::{Duration, Instant};

const DIM0_SIZE: usize = 1024 * 1000;
const DIM1_SIZE: usize = 1024;
const CHUNK0_SIZE: usize = 20;
const CHUNK1_SIZE: usize = 20;

fn write_om_file(file: &str, data: Rc<Vec<f32>>) {
    OmFileWriter::new(DIM0_SIZE, DIM1_SIZE, CHUNK0_SIZE, CHUNK1_SIZE)
        .write_to_file(file, CompressionType::P4nzdec256, 1.0, true, |dim0pos| {
            let start = dim0pos * DIM1_SIZE;
            let end = start + CHUNK0_SIZE * DIM1_SIZE;
            Ok(Rc::new(data[start..end].to_owned()))
        })
        .unwrap();
}

pub fn benchmark_in_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("In-memory operations");
    group.sample_size(10);

    let data = Rc::new(
        (0..DIM0_SIZE * DIM1_SIZE)
            .map(|x| x as f32)
            .collect::<Vec<f32>>(),
    );

    group.bench_function("write_in_memory", |b| {
        b.iter_custom(|iters| {
            let mut timer = Timer::new();
            timer.start();
            for _i in 0..iters {
                black_box(
                    OmFileWriter::new(DIM0_SIZE, DIM1_SIZE, CHUNK0_SIZE, CHUNK1_SIZE)
                        .write_all_in_memory(CompressionType::Fpxdec32, 0.1, data.clone())
                        .unwrap(),
                );
            }
            timer.stop();
            timer.elapsed()
        })
    });

    group.finish();
}

pub fn benchmark_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("Write OM file");
    group.sample_size(10);

    let file = "benchmark.om";
    let data = Rc::new(
        (0..DIM0_SIZE * DIM1_SIZE)
            .map(|x| x as f32)
            .collect::<Vec<f32>>(),
    );

    group.bench_function("write_om_file", move |b| {
        b.iter_custom(|iters| {
            let mut timer = Timer::new();
            for _i in 0..iters {
                // don't measure the time it takes to remove the file
                remove_file_if_exists(file);

                timer.start();
                black_box(write_om_file(&file, data.clone()));
                timer.stop();
            }
            timer.elapsed()
        })
    });

    group.finish();
}

pub fn benchmark_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("Read OM file");

    let file = "benchmark.om";
    let reader = OmFileReader::from_file(file).unwrap();

    let dim0_read_size = 256;

    group.bench_function("read_om_file", move |b| {
        b.iter(|| {
            let random_x: usize = rand::thread_rng().gen_range(0..DIM0_SIZE - dim0_read_size);
            let random_y: usize = rand::thread_rng().gen_range(0..DIM1_SIZE);
            let values = reader
                .read_range(
                    Some(random_x..random_x + dim0_read_size),
                    Some(random_y..random_y + 1),
                )
                .expect("Could not read range");

            assert_eq!(values.len(), dim0_read_size);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_in_memory,
    benchmark_write,
    benchmark_read
);
criterion_main!(benches);

struct Timer {
    start: Option<Instant>,
    elapsed: Duration,
}

impl Timer {
    fn new() -> Self {
        Timer {
            start: None,
            elapsed: Duration::new(0, 0),
        }
    }

    fn start(&mut self) {
        if self.start.is_none() {
            self.start = Some(Instant::now());
        }
    }

    fn stop(&mut self) {
        if let Some(start_time) = self.start {
            self.elapsed += start_time.elapsed();
            self.start = None;
        }
    }

    fn elapsed(&self) -> Duration {
        if let Some(start_time) = self.start {
            self.elapsed + start_time.elapsed()
        } else {
            self.elapsed
        }
    }
}

fn remove_file_if_exists(file: &str) {
    if fs::metadata(file).is_ok() {
        fs::remove_file(file).unwrap();
    }
}
