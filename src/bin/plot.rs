use eframe::egui::{self, CentralPanel, Visuals};
use egui::TopBottomPanel;
use egui_plotter::EguiBackend;
use omfiles_rs::io::reader::OmFileReader;
use plotters::prelude::*;
use std::sync::Arc;

struct DataLoader {
    reader: OmFileReader<omfiles_rs::backend::mmapfile::MmapFile>,
    n_timestamps: u64,
}

impl DataLoader {
    fn new(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let reader = OmFileReader::from_file(file_path)?;
        let dims = reader.get_dimensions();
        let n_timestamps = *dims.last().unwrap();

        Ok(Self {
            reader,
            n_timestamps,
        })
    }

    fn get_timestamp_data(
        &self,
        timestamp: u64,
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error>> {
        let dims = self.reader.get_dimensions();
        println!("dims: {:?}", dims);
        let ranges = dims
            .iter()
            .enumerate()
            .map(|(i, &dim)| {
                if i == dims.len() - 1 {
                    timestamp..timestamp + 1
                } else {
                    // if i == 0 {
                    //     timestamp..timestamp + 1
                    // } else {
                    0..dim
                }
            })
            .collect::<Vec<_>>();

        println!("ranges: {:?}", ranges);

        let flat_data = self.reader.read::<f32>(&ranges, None, None).unwrap();
        // .map_err(|e| e.into())?;

        // Reshape into dims[0] x dims[1]
        // let rows = dims[1] as usize;
        // let cols = dims[2] as usize;
        let rows = dims[0] as usize;
        let cols = dims[1] as usize;

        // Create a new 2D vector with the correct dimensions
        let mut result = vec![vec![0.0; cols]; rows];

        // Fill the 2D vector with data
        for i in 0..rows {
            for j in 0..cols {
                result[i][j] = flat_data[i * cols + j];
            }
        }

        Ok(result)
        // Ok(flat_data.chunks(cols).map(|chunk| chunk.to_vec()).collect())
    }
}

struct App {
    data_loader: Arc<DataLoader>,
    current_timestamp: u64,
    plot_data: Vec<Vec<f32>>,
    plot_dimensions: (usize, usize),
    data_dimensions: Vec<u64>,
}

impl App {
    fn new(
        // cc: &eframe::CreationContext<'_>,
        data_loader: Arc<DataLoader>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // // Disable feathering as it causes artifacts
        // let context = &cc.egui_ctx;

        // context.tessellation_options_mut(|tess_options| {
        //     tess_options.feathering = false;
        // });

        let dims = data_loader.reader.get_dimensions().to_vec();
        println!("dimensions {:?}", dims);
        let plot_dimensions = (dims[0] as usize, dims[1] as usize);
        let initial_data = data_loader.get_timestamp_data(0)?;

        Ok(Self {
            data_loader,
            current_timestamp: 0,
            plot_data: initial_data,
            plot_dimensions,
            data_dimensions: dims,
        })
    }

    fn update_plot_data(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.plot_data = self
            .data_loader
            .get_timestamp_data(self.current_timestamp)?;
        Ok(())
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        TopBottomPanel::bottom("playmenu").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if ui.button("←").clicked() && self.current_timestamp > 0 {
                    self.current_timestamp -= 1;
                    if let Err(e) = self.update_plot_data() {
                        eprintln!("Error updating plot data: {}", e);
                    }
                }

                ui.label(format!("Timestamp: {}", self.current_timestamp));

                if ui.button("→").clicked()
                    && self.current_timestamp < self.data_loader.n_timestamps - 1
                {
                    self.current_timestamp += 1;
                    if let Err(e) = self.update_plot_data() {
                        eprintln!("Error updating plot data: {}", e);
                    }
                }
            });
        });
        CentralPanel::default().show(ctx, |ui| {
            let root = EguiBackend::new(ui).into_drawing_area();
            root.fill(&WHITE).unwrap();
            // Create the plot
            // let plot_response =
            // ui.allocate_response(egui::vec2(800.0, 600.0), egui::Sense::hover());

            // let rect = plot_response.rect;

            // let mut plot_buffer = vec![0u8; (rect.width() * rect.height() * 4.0) as usize];
            {
                // Calculate pixel size to fit the data to the available area
                // let pixel_width = rect.width() as f64 / self.plot_dimensions.1 as f64;
                // let pixel_height = rect.height() as f64 / self.plot_dimensions.0 as f64;
                //
                let all_nan = self
                    .plot_data
                    .iter()
                    .all(|row| row.iter().all(|&x| x.is_nan()));
                if all_nan {
                    println!("All values are nan");
                    return;
                }

                let max_value: f32 = *self
                    .plot_data
                    .iter()
                    .flatten()
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();
                let min_value: f32 = *self
                    .plot_data
                    .iter()
                    .flatten()
                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();

                // Draw each data point as a pixel
                for (y, row) in self.plot_data.iter().rev().enumerate() {
                    for (x, &value) in row.iter().enumerate() {
                        let normalized_value = (value - min_value) / (max_value - min_value);
                        let color = viridis_color(normalized_value);

                        // Calculate pixel coordinates
                        let x_pos = (x as f64 * 1.0) as i32;
                        let y_pos = (y as f64 * 1.0) as i32;

                        // Draw a single pixel for each data point
                        root.draw_pixel((x_pos, y_pos), &color).unwrap();
                    }
                }

                root.present().unwrap();
            }

            // // Convert the buffer to egui format and display
            // let image = egui::ColorImage::from_rgba_unmultiplied(
            //     [rect.width() as usize, rect.height() as usize],
            //     &plot_buffer,
            // );
            // let texture = ui
            //     .ctx()
            //     .load_texture("heatmap", image, egui::TextureOptions::LINEAR);
            // ui.image((&texture));
        });

        // Request continuous updates
        ctx.request_repaint();
    }
}

fn viridis_color(v: f32) -> RGBColor {
    // Ensure v is in [0, 1]
    let v = v.clamp(0.0, 1.0);

    // Viridis colormap approximation
    let x = v * 3.0;

    // Red component
    let r = if v < 0.5 {
        0.0
    } else {
        ((v - 0.5) * 2.0).powf(1.5) * 255.0
    };

    // Green component
    let g = if v < 0.4 {
        v * 3.0 * 255.0
    } else {
        (1.0 - (v - 0.4) / 0.6) * 255.0
    };

    // Blue component
    let b = if v < 0.7 {
        255.0 * (1.0 - v.powf(0.5))
    } else {
        0.0
    };

    RGBColor(r as u8, g as u8, b as u8)
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // let data_loader = Arc::new(DataLoader::new("chunk_1546_spatial.om").unwrap());
    let data_loader = Arc::new(DataLoader::new("chunk_1545.om").unwrap());

    let native_options = eframe::NativeOptions {
        // initial_window_size: Some(egui::vec2(1000.0, 800.0)),
        ..Default::default()
    };

    eframe::run_native(
        "Heatmap Viewer",
        native_options,
        Box::new(move |cc| {
            let app = App::new(data_loader.clone()).unwrap();
            Box::new(app) as Box<dyn eframe::App>
        }),
    )
    .unwrap();

    Ok(())
}
