mod metadata;
mod schema;
mod stats;
mod utils;

use clap::{Parser, Subcommand};
use metadata::print_metadata_table;

use schema::print_schema_table;
use stats::print_stats;

#[derive(Parser)]
#[command(author, version, about="Tool to visualize parquet files")]
pub struct Opts {
    #[command(subcommand)] pub cmd: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Print the metadata summary table
    Meta { path: String },
    Schema {
        path: String,
        #[arg(long, help = "Include per-column statistics in the schema table")]
        show_stats: bool,
    },
    Stats {
        path: String,
        #[arg(long, help = "Only show a single row group (0-based index)")]
        row_group: Option<usize>,
        #[arg(long, help = "Include page-level breakdown")] 
        page: bool,
    },
    // … your other commands …
    // Cat { path: String }
    
}

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Meta { path } => print_metadata_table(&path)?,
        Command::Schema { path, show_stats } => print_schema_table(&path, show_stats)?,
        Command::Stats { path, row_group, page } => print_stats(&path, row_group, page)?,
        // …
    }
    Ok(())
}