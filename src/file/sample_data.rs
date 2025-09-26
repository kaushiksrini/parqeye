use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::record_batch::RecordBatch;
use arrow::array::Array;
use std::sync::Arc;

use std::fs::File;

#[derive(Debug, Clone)]
pub struct ParquetSampleData {
    pub flattened_columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub total_columns: usize,
}


impl ParquetSampleData {
    pub fn read_sample_data(file_path: &str) -> Result<ParquetSampleData, Box<dyn std::error::Error>> {
        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        
        // Get the flattened schema
        let schema = builder.schema();
        let flattened_columns = Self::flatten_schema_columns(&schema);
        
        // Build reader and read first 100 rows
        let mut reader = builder.build()?;
        let mut all_rows = Vec::new();
        let mut total_rows_read = 0;
        // TODO: handle with multiple row groups
        const MAX_ROWS: usize = 100;

        while let Some(batch_result) = reader.next() {
            let batch = batch_result?;
            let batch_rows = batch.num_rows();
            
            if total_rows_read + batch_rows > MAX_ROWS {
                // Take only the remaining rows needed
                let needed_rows = MAX_ROWS - total_rows_read;
                let sliced_batch = batch.slice(0, needed_rows);
                let rows = Self::extract_rows_from_batch(&sliced_batch, &flattened_columns)?;
                all_rows.extend(rows);
                break;
            } else {
                let rows = Self::extract_rows_from_batch(&batch, &flattened_columns)?;
                all_rows.extend(rows);
                total_rows_read += batch_rows;
                
                if total_rows_read >= MAX_ROWS {
                    break;
                }
            }
        }

        Ok(ParquetSampleData {
            total_columns: flattened_columns.len(),
            flattened_columns,
            rows: all_rows,
        })
    }

    fn flatten_schema_columns(schema: &arrow::datatypes::Schema) -> Vec<String> {
        let mut flattened = Vec::new();
        
        for field in schema.fields() {
            Self::flatten_field(field, String::new(), &mut flattened);
        }
        
        flattened
    }

    fn flatten_field(field: &arrow::datatypes::Field, prefix: String, flattened: &mut Vec<String>) {
        let field_name = if prefix.is_empty() {
            field.name().clone()
        } else {
            format!("{}.{}", prefix, field.name())
        };

        match field.data_type() {
            arrow::datatypes::DataType::Struct(fields) => {
                for nested_field in fields {
                    Self::flatten_field(nested_field, field_name.clone(), flattened);
                }
            }
            _ => {
                flattened.push(field_name);
            }
        }
    }

    fn extract_rows_from_batch(
        batch: &RecordBatch, 
        _column_names: &[String]
    ) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
        let mut rows = Vec::new();
        let num_rows = batch.num_rows();
        
        for row_idx in 0..num_rows {
            let mut row = Vec::new();
            
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let value = Self::extract_value_from_array(array, row_idx)?;
                row.push(value);
            }
            
            rows.push(row);
        }
        
        Ok(rows)
    }

    fn extract_value_from_array(array: &Arc<dyn Array>, row_idx: usize) -> Result<String, Box<dyn std::error::Error>> {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        if array.is_null(row_idx) {
            return Ok("NULL".to_string());
        }

        match array.data_type() {
            DataType::Boolean => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Int8 => {
                let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Int16 => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::UInt8 => {
                let array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::UInt16 => {
                let array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::UInt32 => {
                let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::UInt64 => {
                let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::LargeUtf8 => {
                let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Date32 => {
                let array = array.as_any().downcast_ref::<Date32Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Date64 => {
                let array = array.as_any().downcast_ref::<Date64Array>().unwrap();
                Ok(array.value(row_idx).to_string())
            }
            DataType::Timestamp(unit, _) => {
                match unit {
                    arrow::datatypes::TimeUnit::Second => {
                        let array = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                        Ok(array.value(row_idx).to_string())
                    }
                    arrow::datatypes::TimeUnit::Millisecond => {
                        let array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                        Ok(array.value(row_idx).to_string())
                    }
                    arrow::datatypes::TimeUnit::Microsecond => {
                        let array = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                        Ok(array.value(row_idx).to_string())
                    }
                    arrow::datatypes::TimeUnit::Nanosecond => {
                        let array = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                        Ok(array.value(row_idx).to_string())
                    }
                }
            }
            // TODO: Handle Dictionary, List, and other types. The columns are not extracted properly either.
            _ => {
                Ok(format!("O {:?}", array.slice(row_idx, 1)))
            }
        }
    }
}