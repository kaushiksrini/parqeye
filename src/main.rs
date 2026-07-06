use parqeye::app::App;
use parqeye::file::error::FileIOError;
use parqeye::file::parquet_ctx::ParquetCtx;

use clap::Parser;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Command line tool to visualize parquet files"
)]
pub struct Opts {
    /// Path to the parquet file
    pub path: String,
}

fn main() {
    let opts = Opts::parse();
    if let Err(e) = run(&opts.path) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

fn run(path: &str) -> Result<(), FileIOError> {
    let file_info = ParquetCtx::from_file(path)?;

    let mut terminal = ratatui::init();
    let mut app = App::new(&file_info);
    let result = app.run(&mut terminal);
    ratatui::restore();

    result.map_err(|e| FileIOError::Io { source: e })
}
