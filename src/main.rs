use parqeye::app::App;
use parqeye::file::parquet_ctx::ParquetCtx;
use std::io;

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

fn main() -> io::Result<()> {
    let opts = Opts::parse();
    tui(&opts.path)?;
    Ok(())
}

fn tui(path: &str) -> io::Result<()> {
    let mut terminal = ratatui::init();

    let file_info = ParquetCtx::from_file(path).map_err(|e| io::Error::other(e.to_string()))?;

    let mut app = App::new(&file_info);
    app.run(&mut terminal)?;
    ratatui::restore();
    Ok(())
}
