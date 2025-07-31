use std::io;
use datatools::app::App;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about="Tool to visualize parquet files")]
pub struct Opts {
    #[command(subcommand)] pub cmd: Command,
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
    let app_result = App::default().run(&mut terminal, path);
    ratatui::restore();
    app_result
}