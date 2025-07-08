use anyhow::Result;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::reader::RowGroupReader;
use parquet::basic::PageType;
use comfy_table::{Table, presets::UTF8_FULL, modifiers::UTF8_ROUND_CORNERS, Cell, Color, Attribute};
use std::fs::File;
use crate::utils::human_readable_bytes;

/// Entry point to print stats. If `row_group_idx` is Some, only that row group is shown.
/// If `show_pages` is true, page-level breakdown is printed per column.
pub fn print_stats(path: &str, row_group_idx: Option<usize>, show_pages: bool) -> Result<()> {
    // open file
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let md = reader.metadata();
    let schema = md.file_metadata().schema_descr();

    let rg_indices: Vec<usize> = match row_group_idx {
        Some(idx) => vec![idx],
        None => (0..reader.num_row_groups()).collect(),
    };

    for rg_i in rg_indices {
        let rg_reader = reader.get_row_group(rg_i)?;
        let rg_meta = rg_reader.metadata();

        println!("\nRow Group {}  (rows={}, total_byte_size={})", rg_i, rg_meta.num_rows(), rg_meta.total_byte_size());

        // Build a table with column chunk level stats
        let mut table = Table::new();
        table.load_preset(UTF8_FULL).apply_modifier(UTF8_ROUND_CORNERS);
        table.set_header(vec![
            Cell::new("Col#").fg(Color::Green).add_attribute(Attribute::Bold),
            Cell::new("Path").fg(Color::Green).add_attribute(Attribute::Bold),
            Cell::new("Codec").fg(Color::Green).add_attribute(Attribute::Bold),
            Cell::new("Encodings").fg(Color::Green).add_attribute(Attribute::Bold),
            Cell::new("Pages").fg(Color::Green).add_attribute(Attribute::Bold),
            Cell::new("Uncomp").fg(Color::Green).add_attribute(Attribute::Bold),
            Cell::new("Comp").fg(Color::Green).add_attribute(Attribute::Bold),
            Cell::new("Ratio").fg(Color::Green).add_attribute(Attribute::Bold),
        ]);

        for col_idx in 0..rg_reader.num_columns() {
            let col_meta = rg_meta.column(col_idx);
            let col_path = schema.column(col_idx).path().to_string();
            let codec = format!("{:?}", col_meta.compression());
            let mut encs: Vec<String> = col_meta
                .encodings()
                .iter()
                .map(|e| format!("{:?}", e))
                .collect();
            encs.sort();
            let enc_summary = encs.join(",");

            // Count pages quickly by iterating reader if requested, or else from column index.
            let mut page_count = 0;
            if show_pages {
                let mut page_reader = rg_reader.get_column_page_reader(col_idx)?;
                while let Some(page) = page_reader.get_next_page()? {
                    if matches!(page.page_type(), PageType::DATA_PAGE | PageType::DATA_PAGE_V2) {
                        page_count += 1;
                    }
                }
            } else {
                page_count = col_meta.num_values() as usize; // fallback (approx) but placeholder
            }

            table.add_row(vec![
                Cell::new(col_idx).fg(Color::Cyan),
                Cell::new(col_path).fg(Color::Cyan),
                Cell::new(codec),
                Cell::new(enc_summary),
                Cell::new(page_count),
                Cell::new(human_readable_bytes(col_meta.uncompressed_size() as u64)),
                Cell::new(human_readable_bytes(col_meta.compressed_size() as u64)),
                Cell::new(format!("{:.2}×", col_meta.uncompressed_size() as f64 / col_meta.compressed_size() as f64)),
            ]);
        }

        println!("{}", table);

        if show_pages {
            // Detailed per-page stats per column
            println!("{}", "=".repeat(80));
            println!("{}", rg_reader.num_columns());
            // println!("{}", "Page-level stats".to_string().fg(Color::Green).add_attribute(Attribute::Bold));
            for col_idx in 0..rg_reader.num_columns() {
                let col_meta = rg_meta.column(col_idx);
                let col_path = col_meta.column_descr().path().to_string();
                println!("  Column {} ({}) pages:", col_idx, col_path);

                let mut page_reader = rg_reader.get_column_page_reader(col_idx)?;
                let mut pg_index = 0;
                while let Some(page) = page_reader.get_next_page()? {
                    match page.page_type() {
                        PageType::DATA_PAGE => {
                            let num_vals = page.num_values();
                            let stats_avail = page.statistics().is_some();
                            println!("    page {:>3}: values={}, stats={:?}", pg_index, num_vals, page.statistics());
                            // println!("    page {:>3}: values={}, stats={:?}", pg_index, page, page.statistics());
                        }
                        PageType::DATA_PAGE_V2 => {
                            let num_vals = page.num_values();
                            let stats_avail = page.statistics().is_some();
                            // println!("    page {:>3}: values={}, ", pg_index, page.);
                        }
                        PageType::DICTIONARY_PAGE => {
                            println!("    page {:>3}: <dict page>: {:?}", pg_index, page.statistics());
                        }
                        _ => {}
                    }
                    pg_index += 1;
                }
            }
        }
    }

    Ok(())
}
