mod gh;
mod pr;

use std::collections::HashMap;
use std::fs::{File, create_dir_all};
use std::path::Path;
use std::sync::{Arc, Mutex};
use anyhow::{Result, Context};
use indicatif::{ProgressBar, ProgressStyle};
use clap::Parser;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Row, RowAccessor};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::file::properties::WriterProperties;
use parquet::basic::Compression;
use parquet::schema::types::Type;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::{ByteArray, Int64Type, ByteArrayType};
use serde_json::Value;
use chrono::{DateTime, Utc, Datelike};

#[derive(Parser)]
#[command(name = "git-history-exporter")]
#[command(about = "Export and process Git history archives")]
struct Args {
    /// Timeframe to process (YYYY, YYYY-MM, or YYYY-MM-DD)
    timeframe: String,
}

fn extract_month_from_created_at(created_at_millis: i64) -> Result<String> {
    // Simple conversion - just extract year-month from timestamp
    let dt = std::time::UNIX_EPOCH + std::time::Duration::from_millis(created_at_millis as u64);
    let datetime = chrono::DateTime::<chrono::Utc>::from(dt);
    Ok(format!("{:04}-{:02}", datetime.year(), datetime.month()))
}

fn get_bucket_key(repo_name: &str, month: &str) -> String {
    let repo_prefix = if repo_name.len() >= 3 {
        &repo_name[..3]
    } else {
        repo_name
    };
    
    let safe_repo_prefix = repo_prefix.replace('/', "_");
    
    let mut path_parts = Vec::new();
    for ch in safe_repo_prefix.chars() {
        path_parts.push(ch.to_string());
    }
    
    path_parts.push(month.to_string());
    path_parts.join("/")
}

fn parse_timeframe(timeframe: &str) -> Result<Vec<String>> {
    let parts: Vec<&str> = timeframe.split('-').collect();
    
    match parts.len() {
        1 => {
            // For year-only, use the year as prefix to match files like "2024-000000whatever"
            Ok(vec![parts[0].to_string()])
        },
        2 => Ok(vec![format!("{}-{}", parts[0], parts[1])]),
        3 => Ok(vec![format!("{}-{}", parts[0], parts[1])]),
        _ => Err(anyhow::anyhow!("Invalid timeframe format. Use YYYY, YYYY-MM, or YYYY-MM-DD")),
    }
}

fn find_parquet_files(timeframe_patterns: &[String]) -> Result<Vec<String>> {
    let mut files = Vec::new();
    
    for pattern in timeframe_patterns {
        let dir_path = Path::new("work/archives-bq");
        if !dir_path.exists() {
            return Err(anyhow::anyhow!("Directory work/archives-bq does not exist"));
        }
        
        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();
            
            if file_name_str.starts_with(pattern) && file_name_str.ends_with(".parquet.zst") {
                files.push(entry.path().to_string_lossy().to_string());
            }
        }
    }
    
    files.sort();
    Ok(files)
}

#[derive(Debug)]
struct RowBuffer {
    event_types: Vec<String>,
    payloads: Vec<String>,
    repo_names: Vec<String>,
    created_ats: Vec<i64>,
}

impl RowBuffer {
    fn new() -> Self {
        Self {
            event_types: Vec::new(),
            payloads: Vec::new(),
            repo_names: Vec::new(),
            created_ats: Vec::new(),
        }
    }
    
    fn add_row(&mut self, event_type: String, payload: String, repo_name: String, created_at: i64) {
        self.event_types.push(event_type);
        self.payloads.push(payload);
        self.repo_names.push(repo_name);
        self.created_ats.push(created_at);
    }
    
    fn len(&self) -> usize {
        self.event_types.len()
    }
    
    fn clear(&mut self) {
        self.event_types.clear();
        self.payloads.clear();
        self.repo_names.clear();
        self.created_ats.clear();
    }
}

type ParquetWriters = Arc<Mutex<HashMap<String, (SerializedFileWriter<File>, RowBuffer)>>>;

fn get_or_create_parquet_writer(writers: &ParquetWriters, bucket_key: &str) -> Result<()> {
    let mut writers_map = writers.lock().unwrap();
    
    if !writers_map.contains_key(bucket_key) {
        let parts: Vec<&str> = bucket_key.split('/').collect();
        if parts.len() < 2 {
            return Err(anyhow::anyhow!("Invalid bucket key format: '{}'", bucket_key));
        }
        
        let dir_parts = &parts[..parts.len()-1];
        let month = parts[parts.len()-1];
        
        let repo_dir = format!("work/archives-separated/{}", dir_parts.join("/"));
        create_dir_all(&repo_dir)?;
        
        let path = format!("{}/{}.parquet", repo_dir, month);
        
        let file = File::create(&path)?;

        let schema = Arc::new(parse_message_type(OUTPUT_SCHEMA)?);
        
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        
        let writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;
        let buffer = RowBuffer::new();
        writers_map.insert(bucket_key.to_string(), (writer, buffer));
    }
    
    Ok(())
}

fn extract_data_from_parquet_row(row: &Row) -> Result<Option<(String, String, String, i64)>> {
    // Extract event type
    let event_type = row.get_string(0)?.to_string();

    let repo_group = row.get_group(3)?;
    let repo_name = repo_group.get_string(1)?.to_string();

    let payload = row.get_string(2)?.to_string();
    
    // Extract created_at timestamp
    let created_timestamp = row.get_timestamp_micros(6)? / 1000;
    
    Ok(Some((event_type, repo_name, payload, created_timestamp)))
}

const OUTPUT_SCHEMA: &str = r#"
message schema {
  REQUIRED BYTE_ARRAY type (STRING);
  REQUIRED BYTE_ARRAY payload (STRING);
  REQUIRED BYTE_ARRAY repo_name (STRING);
  REQUIRED INT64 created_at;
}
"#;

fn process_parquet_file(file_path: &str, parquet_writers: ParquetWriters) -> Result<()> {
    let file = File::open(file_path)
        .context(format!("Failed to open parquet file: {}", file_path))?;
    
    let reader = SerializedFileReader::new(file)?;
    
    let spinner = ProgressBar::new_spinner();
    spinner.set_message(format!("Processing {}", Path::new(file_path).file_name().unwrap().to_string_lossy()));
    spinner.set_style(ProgressStyle::default_spinner()
        .template("{spinner:.green} {msg} [{elapsed_precise}] {human_pos} rows processed ({per_sec})")?);
    
    let mut row_iter = reader.get_row_iter(None)?;

    let schema = reader.metadata().file_metadata().schema();
    
    while let Some(row) = row_iter.next() {
        let row = row?;
        
        // Extract data directly from parquet row without JSON conversion
        if let Some((event_type, repo_name, payload, created_at)) = extract_data_from_parquet_row(&row)? {
            let month = extract_month_from_created_at(created_at)?;
            let bucket_key = get_bucket_key(&repo_name, &month);
            
            // Pass the original row directly instead of converting to JSON
            write_row_to_parquet(&parquet_writers, &bucket_key, &row)?;
        } else {
            println!("No data found in row");
        }
        
        spinner.inc(1);
    }
    
    spinner.finish();
    Ok(())
}

fn write_row_to_parquet(writers: &ParquetWriters, bucket_key: &str, row: &Row) -> Result<()> {
    get_or_create_parquet_writer(writers, bucket_key)?;
    
    // Extract the data we need from the row
    let (event_type, repo_name, payload, created_at) = extract_data_from_parquet_row(row)?.unwrap();
    
    // Add to buffer
    {
        let mut writers_map = writers.lock().unwrap();
        let (_, buffer) = writers_map.get_mut(bucket_key).unwrap();
        buffer.add_row(event_type, payload, repo_name, created_at);
        
        // Write batch when buffer reaches threshold
        if buffer.len() >= 1000 {
            flush_buffer_to_parquet(&mut writers_map.get_mut(bucket_key).unwrap())?;
        }
    }
    
    Ok(())
}

fn flush_buffer_to_parquet((writer, buffer): &mut (SerializedFileWriter<File>, RowBuffer)) -> Result<()> {
    if buffer.len() == 0 {
        return Ok(());
    }
    
    let mut row_group_writer = writer.next_row_group()?;
    
    // Write event_type column (type)
    {
        let mut col_writer = row_group_writer.next_column()?.unwrap();
        let values: Vec<parquet::data_type::ByteArray> = buffer.event_types.iter()
            .map(|s| parquet::data_type::ByteArray::from(s.as_bytes()))
            .collect();
        col_writer.typed::<parquet::data_type::ByteArrayType>()
            .write_batch(&values, None, None)?;
        col_writer.close()?;
    }
    
    // Write payload column  
    {
        let mut col_writer = row_group_writer.next_column()?.unwrap();
        let values: Vec<parquet::data_type::ByteArray> = buffer.payloads.iter()
            .map(|s| parquet::data_type::ByteArray::from(s.as_bytes()))
            .collect();
        col_writer.typed::<parquet::data_type::ByteArrayType>()
            .write_batch(&values, None, None)?;
        col_writer.close()?;
    }
    
    // Write repo name column
    {
        let mut col_writer = row_group_writer.next_column()?.unwrap();
        let values: Vec<parquet::data_type::ByteArray> = buffer.repo_names.iter()
            .map(|s| parquet::data_type::ByteArray::from(s.as_bytes()))
            .collect();
        col_writer.typed::<parquet::data_type::ByteArrayType>()
            .write_batch(&values, None, None)?;
        col_writer.close()?;
    }
    
    // Write created_at column
    {
        let mut col_writer = row_group_writer.next_column()?.unwrap();
        col_writer.typed::<parquet::data_type::Int64Type>()
            .write_batch(&buffer.created_ats, None, None)?;
        col_writer.close()?;
    }
    
    row_group_writer.close()?;
    buffer.clear();
    
    Ok(())
}

fn finalize_parquet_writers(writers: ParquetWriters) -> Result<()> {
    let writers_map = Arc::try_unwrap(writers)
        .map_err(|_| anyhow::anyhow!("Failed to extract writers"))?
        .into_inner()
        .unwrap();
    
    let spinner = ProgressBar::new(writers_map.len() as u64);
    spinner.set_message("Finalizing parquet files");
    spinner.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>3}/{len:3} {msg}")
        .unwrap()
        .progress_chars("##-"));
    
    for (bucket_key, mut writer_buffer) in writers_map {
        // Flush any remaining data in the buffer
        if writer_buffer.1.len() > 0 {
            flush_buffer_to_parquet(&mut writer_buffer)?;
        }
        // Ensure the writer is properly closed
        let writer = writer_buffer.0;
        writer.close()?;
        spinner.inc(1);
    }
    
    spinner.finish_with_message("All parquet files finalized");
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    
    let timeframe = &args.timeframe;
    
    let timeframe_patterns = parse_timeframe(timeframe)?;
    let parquet_files = find_parquet_files(&timeframe_patterns)?;
    
    if parquet_files.is_empty() {
        return Err(anyhow::anyhow!("No parquet files found for timeframe: {}", timeframe));
    }
    
    create_dir_all("work/archives-separated")?;
    
    println!("Processing {} parquet files for timeframe: {}", parquet_files.len(), timeframe);
    
    let main_pb = ProgressBar::new(parquet_files.len() as u64);
    main_pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}/{duration_precise}] {bar:40.cyan/blue} {pos:>3}/{len:3} {msg}")
            .unwrap()
            .progress_chars("##-")
    );
    main_pb.set_message("Processing parquet files");
    
    let parquet_writers: ParquetWriters = Arc::new(Mutex::new(HashMap::new()));
    
    for file_path in &parquet_files {
        main_pb.set_message(format!("Processing {}", Path::new(&file_path).file_name().unwrap().to_string_lossy()));
        
        match process_parquet_file(&file_path, Arc::clone(&parquet_writers)) {
            Ok(_) => {
                main_pb.println(format!("✓ Successfully processed {}", file_path));
            }
            Err(e) => {
                main_pb.println(format!("✗ Failed to process {}: {}", file_path, e));
            }
        }
        
        main_pb.inc(1);
    }
    
    main_pb.finish_with_message("All parquet files processed");
    
    println!("Finalizing parquet files...");
    finalize_parquet_writers(parquet_writers)?;
    
    println!("✓ All processing complete!");
    
    Ok(())
}