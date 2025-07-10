mod gh;
mod pr;

use std::collections::HashMap;
use std::fs::{File, create_dir_all, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use flate2::read::GzDecoder;
use zstd::Encoder;
use anyhow::{Result, Context};
use indicatif::{ProgressBar, ProgressStyle};
use std::env;
use std::sync::{Arc, Mutex};
use clap::Parser;

#[derive(Parser)]
#[command(name = "git-history-exporter")]
#[command(about = "Export and process Git history archives")]
struct Args {
    /// Timeframe to process (YYYY, YYYY-MM, or YYYY-MM-DD)
    timeframe: String,
    
    /// ZSTD compression level (0-22, where 0 disables compression)
    #[arg(long, default_value = "4", value_parser = clap::value_parser!(u8).range(0..=22))]
    zstd_level: u8,
}

fn extract_type_and_repo(line: &str) -> Option<(&str, &str)> {
    // Find type first - it appears early in the line
    let type_start = line.find(r#""type":""#)? + 8;
    let type_end = line[type_start..].find('"')? + type_start;
    let event_type = &line[type_start..type_end];
    
    // Then find repo
    let repo_start = line.find(r#""repo":{"id":"#)?;
    let name_start = line[repo_start..].find(r#","name":""#)? + repo_start + 9;
    let name_end = line[name_start..].find('"')? + name_start;
    let repo = &line[name_start..name_end];
    
    Some((event_type, repo))
}

fn extract_month_from_datetime(datetime: &str) -> Result<String> {
    // Extract YYYY-MM from datetime format YYYY-MM-DD-HH
    let parts: Vec<&str> = datetime.split('-').collect();
    if parts.len() < 2 {
        return Err(anyhow::anyhow!("Invalid datetime format"));
    }
    Ok(format!("{}-{}", parts[0], parts[1]))
}

fn get_bucket_key(repo_name: &str, month: &str) -> String {
    // Get first 3 characters of repo name (handle case where repo name is shorter)
    let repo_prefix = if repo_name.len() >= 3 {
        &repo_name[..3]
    } else {
        repo_name
    };
    
    // Replace slashes with underscores to avoid invalid bucket key format
    let safe_repo_prefix = repo_prefix.replace('/', "_");
    
    // Create branching structure: each character becomes a directory level
    let mut path_parts = Vec::new();
    for ch in safe_repo_prefix.chars() {
        path_parts.push(ch.to_string());
    }
    
    // Join with month at the end
    path_parts.push(month.to_string());
    path_parts.join("/")
}

fn parse_timeframe(timeframe: &str) -> Result<Vec<String>> {
    let parts: Vec<&str> = timeframe.split('-').collect();
    let mut datetimes = Vec::new();
    
    match parts.len() {
        1 => {
            // Year format: "2021"
            let year: i32 = parts[0].parse()
                .context("Invalid year format")?;
            
            // Generate all hours for the entire year
            for month in 1..=12 {
                let days_in_month = match month {
                    1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                    4 | 6 | 9 | 11 => 30,
                    2 => if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 29 } else { 28 },
                    _ => return Err(anyhow::anyhow!("Invalid month")),
                };
                
                for day in 1..=days_in_month {
                    for hour in 0..24 {
                        datetimes.push(format!("{:04}-{:02}-{:02}-{}", year, month, day, hour));
                    }
                }
            }
        },
        2 => {
            // Month format: "2023-09"
            let year: i32 = parts[0].parse()
                .context("Invalid year in month format")?;
            let month: u32 = parts[1].parse()
                .context("Invalid month")?;
            
            if month < 1 || month > 12 {
                return Err(anyhow::anyhow!("Month must be between 1 and 12"));
            }
            
            let days_in_month = match month {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                2 => if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 29 } else { 28 },
                _ => return Err(anyhow::anyhow!("Invalid month")),
            };
            
            // Generate all hours for the entire month
            for day in 1..=days_in_month {
                for hour in 0..24 {
                    datetimes.push(format!("{:04}-{:02}-{:02}-{}", year, month, day, hour));
                }
            }
        },
        3 => {
            // Day format: "2024-06-05"
            let year: i32 = parts[0].parse()
                .context("Invalid year in day format")?;
            let month: u32 = parts[1].parse()
                .context("Invalid month in day format")?;
            let day: u32 = parts[2].parse()
                .context("Invalid day")?;
            
            if month < 1 || month > 12 {
                return Err(anyhow::anyhow!("Month must be between 1 and 12"));
            }
            
            let days_in_month = match month {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                2 => if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 29 } else { 28 },
                _ => return Err(anyhow::anyhow!("Invalid month")),
            };
            
            if day < 1 || day > days_in_month {
                return Err(anyhow::anyhow!("Day must be between 1 and {}", days_in_month));
            }
            
            // Generate all hours for the single day
            for hour in 0..24 {
                datetimes.push(format!("{:04}-{:02}-{:02}-{}", year, month, day, hour));
            }
        },
        _ => {
            return Err(anyhow::anyhow!("Invalid timeframe format. Use YYYY, YYYY-MM, or YYYY-MM-DD"));
        }
    }
    
    Ok(datetimes)
}

type FileWriters = Arc<Mutex<HashMap<String, Box<dyn Write + Send>>>>;

fn get_or_create_writer(writers: &FileWriters, bucket_key: &str, zstd_level: u8) -> Result<()> {
    let mut writers_map = writers.lock().unwrap();
    
    if !writers_map.contains_key(bucket_key) {
        // Extract directory and filename from bucket_key (format: "char1/char2/char3/YYYY-MM")
        let parts: Vec<&str> = bucket_key.split('/').collect();
        if parts.len() < 2 {
            return Err(anyhow::anyhow!("Invalid bucket key format: '{}' (expected at least 'char/YYYY-MM', got {} parts)", bucket_key, parts.len()));
        }
        
        // All parts except the last one form the directory path
        let dir_parts = &parts[..parts.len()-1];
        let month = parts[parts.len()-1];
        
        // Create nested directory structure
        let repo_dir = format!("work/archives-separated/{}", dir_parts.join("/"));
        create_dir_all(&repo_dir)?;
        
        // Create file with appropriate extension based on compression level
        let path = if zstd_level == 0 {
            format!("{}/{}.jsonl", repo_dir, month)
        } else {
            format!("{}/{}.jsonl.zstd", repo_dir, month)
        };
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        
        let writer: Box<dyn Write + Send> = if zstd_level == 0 {
            Box::new(BufWriter::new(file))
        } else {
            let encoder = Encoder::new(file, zstd_level as i32)?;
            Box::new(BufWriter::new(encoder))
        };
        
        writers_map.insert(bucket_key.to_string(), writer);
    }
    
    Ok(())
}

fn write_line_to_bucket(writers: &FileWriters, bucket_key: &str, line: &str, zstd_level: u8) -> Result<()> {
    get_or_create_writer(writers, bucket_key, zstd_level)?;
    
    let mut writers_map = writers.lock().unwrap();
    if let Some(writer) = writers_map.get_mut(bucket_key) {
        writeln!(writer, "{}", line)?;
    }
    
    Ok(())
}

fn process_archive(datetime: &str, file_writers: FileWriters, zstd_level: u8) -> Result<()> {
    let path = format!("work/archives/{}.json.gz", datetime);

    let file = File::open(&path).context(format!("Failed to open archive file from {}", path))?;

    let spinner = ProgressBar::new_spinner();
    spinner.set_message(format!("Processing {}", datetime));
    spinner.set_style(ProgressStyle::default_spinner().template("{spinner:.green} {msg} [{elapsed_precise}] {human_pos} completed ({per_sec})")?);
        
    // Extract month from datetime for bucketing
    let month = extract_month_from_datetime(datetime)?;
    
    // Decompress the gzipped content
    let decoder = GzDecoder::new(file);
    
    let reader = BufReader::new(decoder);

    for line in reader.lines() {
        let line = line?;
        if let Some((event_type, repo_name)) = extract_type_and_repo(&line) {
            match event_type {
                "PullRequestEvent" | "PushEvent" | "PullRequestReviewEvent" |
                "PullRequestReviewCommentEvent" | "PullRequestReviewThreadEvent" => {
                    let bucket_key = get_bucket_key(repo_name, &month);
                    write_line_to_bucket(&file_writers, &bucket_key, &line, zstd_level)?;
                }
                _ => {}
            }
        }
        spinner.inc(1);
    }
    
    spinner.finish();

    Ok(())
}

fn flush_and_close_writers(writers: FileWriters) -> Result<()> {
    let writers_map = Arc::try_unwrap(writers)
        .map_err(|_| anyhow::anyhow!("Failed to extract writers"))?
        .into_inner()
        .unwrap();
    
    let spinner = ProgressBar::new(writers_map.len() as u64);
    spinner.set_message("Flushing and closing files");
    spinner.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>3}/{len:3} {msg}")
        .unwrap()
        .progress_chars("##-"));
    
    for (bucket_key, mut writer) in writers_map {
        writer.flush().context(format!("Failed to flush writer for {}", bucket_key))?;
        spinner.inc(1);
    }
    
    spinner.finish_with_message("All files flushed and closed");
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    
    let timeframe = &args.timeframe;
    let zstd_level = args.zstd_level;
    
    let datetimes = parse_timeframe(timeframe)?;
    
    // Create the separated directory if it doesn't exist
    create_dir_all("work/archives-separated")?;
    
    println!("Processing {} archive files for timeframe: {}", datetimes.len(), timeframe);
    
    let main_pb = ProgressBar::new(datetimes.len() as u64);
    main_pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}/{duration_precise}] {bar:40.cyan/blue} {pos:>3}/{len:3} {msg}")
            .unwrap()
            .progress_chars("##-")
    );
    main_pb.set_message("Processing archives");
    
    // Shared file writers for all buckets
    let file_writers: FileWriters = Arc::new(Mutex::new(HashMap::new()));
    
    for datetime in datetimes {
        main_pb.set_message(format!("Processing {}", datetime));
        
        match process_archive(&datetime, Arc::clone(&file_writers), zstd_level) {
            Ok(_) => {
                main_pb.println(format!("✓ Successfully processed {}", datetime));
            }
            Err(e) => {
                main_pb.println(format!("✗ Failed to process {}: {}", datetime, e));
            }
        }
        
        main_pb.inc(1);
    }
    
    main_pb.finish_with_message("All archives processed");
    
    // Flush and close all file writers
    println!("Flushing and closing files...");
    flush_and_close_writers(file_writers)?;
    
    println!("✓ All processing complete!");
    
    Ok(())
}