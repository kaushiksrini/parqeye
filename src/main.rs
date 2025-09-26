use datatools::app::App;
use datatools::file::parquet_ctx::ParquetCtx;
use std::io;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Command line tool to visualize parquet files"
)]
pub struct Opts {
    #[command(subcommand)]
    pub cmd: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Tui { path: String },
}

fn main() -> io::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Tui { path } => tui(&path)?,
    }
    Ok(())
}

fn tui(path: &str) -> io::Result<()> {
    let mut terminal = ratatui::init();

    let file_info = ParquetCtx::from_file(path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    let mut app = App::new(&file_info);
    app.run(&mut terminal)?;
    ratatui::restore();
    Ok(())
}
