use anyhow::{Context, Result};
use clap::Parser;
use git2::{Repository, Commit, DiffOptions, ObjectType, Oid, DiffDelta};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the git repository directory
    repo_path: PathBuf,
    
    /// Output JSON file path
    #[arg(short, long)]
    output: Option<PathBuf>,
    
    /// Pretty-print JSON output
    #[arg(long)]
    pretty: bool,
    
    /// Suppress output messages and progress bars
    #[arg(long)]
    silent: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct CommitInfo {
    commit_hash: String,
    commit_message: String,
    diff: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct FileInfo {
    #[serde(rename = "currentContents")]
    current_contents: String,
    history: Vec<CommitInfo>,
}

type ExportData = HashMap<String, FileInfo>;

fn main() -> Result<()> {
    let args = Args::parse();
    
    // Set default output file to "history_exported.json" within the repo directory
    let output_path = args.output.unwrap_or_else(|| args.repo_path.join("history_exported.json"));
    
    if !args.silent {
        println!("Exporting Git repository from: {}", args.repo_path.display());
        println!("Output file: {}", output_path.display());
    }
    
    let repo = Repository::open(&args.repo_path)
        .with_context(|| format!("Failed to open repository at {}", args.repo_path.display()))?;
    
    // Pre-allocate HashMap with estimated capacity to reduce reallocations
    let mut export_data: ExportData = HashMap::with_capacity(1000);
    
    // First, process commits to discover all files that have ever existed
    // This will also build up the history for all files
    process_commit_history(&repo, &mut export_data, args.silent)?;
    
    // Now get current contents for files that still exist
    populate_current_contents(&repo, &args.repo_path, &mut export_data, args.silent)?;
    
    // Write to JSON file
    let json_output = if args.pretty {
        serde_json::to_string_pretty(&export_data)
            .context("Failed to serialize data to JSON")?
    } else {
        serde_json::to_string(&export_data)
            .context("Failed to serialize data to JSON")?
    };
    
    fs::write(&output_path, json_output)
        .with_context(|| format!("Failed to write to output file {}", output_path.display()))?;
    
    if !args.silent {
        println!("Successfully exported {} files to {}", export_data.len(), output_path.display());
    }
    
    Ok(())
}

fn process_commit_history(repo: &Repository, export_data: &mut ExportData, silent: bool) -> Result<()> {
    let mut revwalk = repo.revwalk()?;
    
    // Start from HEAD and walk backwards through history
    revwalk.push_head()?;
    revwalk.set_sorting(git2::Sort::TIME | git2::Sort::REVERSE)?; // REVERSE for chronological order
    
    // Get total count for progress bar (this is much more memory efficient)
    let total_commits = {
        let mut count_walk = repo.revwalk()?;
        count_walk.push_head()?;
        count_walk.count()
    };
    
    let commit_pb = if !silent {
        let pb = ProgressBar::new(total_commits as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}/{eta_precise}] {bar:40.green/blue} {pos:>7}/{len:7} {msg} [{per_sec}]")
                .unwrap()
                .progress_chars("##-")
        );
        pb.set_message("Processing commits");
        Some(pb)
    } else {
        None
    };
    
    // Process commits as we iterate (streaming)
    let mut processed_count = 0;
    let update_interval = std::cmp::max(1, total_commits / 100); // Update every 1% of commits
    
    for commit_id in revwalk {
        let commit_id = commit_id?;
        let commit = repo.find_commit(commit_id)?;
        let parent_id = if commit.parent_count() > 0 {
            Some(commit.parent(0)?.id())
        } else {
            None
        };
        
        // Get the diff for this commit
        let modified_files = get_commit_file_changes(repo, &commit, parent_id)?;
        
        for (file_path, diff) in modified_files {
            // Skip .git directory and other hidden files
            if file_path.starts_with(".git") || file_path.starts_with('.') {
                continue;
            }
            
            // Use entry API to avoid double HashMap lookup
            let file_info = export_data.entry(file_path.clone()).or_insert_with(|| FileInfo {
                current_contents: String::new(), // Will be populated later
                history: Vec::with_capacity(16), // Pre-allocate reasonable capacity
            });
            
            // Add to history
            file_info.history.push(CommitInfo {
                commit_hash: commit.id().to_string(),
                commit_message: commit.message().unwrap_or("").to_string(),
                diff,
            });
        }
        
        processed_count += 1;
        // Batch update progress bar for better performance
        if processed_count % update_interval == 0 || processed_count == total_commits {
            if let Some(pb) = &commit_pb {
                pb.set_position(processed_count as u64);
            }
        }
    }
    
    if let Some(pb) = commit_pb {
        pb.finish_with_message("Finished processing commits");
    }
    
    Ok(())
}

fn get_commit_file_changes(
    repo: &Repository,
    commit: &Commit,
    parent_id: Option<Oid>,
) -> Result<HashMap<String, String>> {
    let mut file_changes = HashMap::new();
    
    let current_tree = commit.tree()?;
    
    if let Some(parent_id) = parent_id {
        let parent_commit = repo.find_commit(parent_id)?;
        let parent_tree = parent_commit.tree()?;
        
        let diff = repo.diff_tree_to_tree(Some(&parent_tree), Some(&current_tree), None)?;
        
        // Process the full diff once and extract content for each file
        diff.print(git2::DiffFormat::Patch, |delta, _hunk, line| {
            if let Some(file_path) = get_file_path_from_delta(&delta) {
                // Use entry API to avoid multiple HashMap lookups
                let diff_content = file_changes.entry(file_path).or_insert_with(|| String::with_capacity(1024));
                
                // Append line content directly without intermediate allocations
                diff_content.push_str(std::str::from_utf8(line.content()).unwrap_or(""));
            }
            true
        })?;
    } else {
        // First commit - all files are additions
        let mut diff_options = DiffOptions::new();
        diff_options.include_untracked(true);
        
        let diff = repo.diff_tree_to_tree(None, Some(&current_tree), Some(&mut diff_options))?;
        
        diff.foreach(
            &mut |delta, _| {
                if let Some(file_path) = get_file_path_from_delta(&delta) {
                    if let Ok(entry) = current_tree.get_path(Path::new(&file_path)) {
                        if let Ok(object) = entry.to_object(repo) {
                            if object.kind() == Some(ObjectType::Blob) {
                                let blob = object.as_blob().unwrap();
                                let content = String::from_utf8_lossy(blob.content());
                                
                                // Pre-allocate string capacity based on content size
                                let mut diff_text = String::with_capacity(content.len() + content.lines().count());
                                for line in content.lines() {
                                    diff_text.push('+');
                                    diff_text.push_str(line);
                                    diff_text.push('\n');
                                }
                                file_changes.insert(file_path, diff_text);
                            }
                        }
                    }
                }
                true
            },
            None,
            None,
            None,
        )?;
    }
    
    Ok(file_changes)
}

fn get_file_path_from_delta(delta: &DiffDelta) -> Option<String> {
    if let Some(new_file) = delta.new_file().path() {
        Some(new_file.to_string_lossy().to_string())
    } else if let Some(old_file) = delta.old_file().path() {
        Some(old_file.to_string_lossy().to_string())
    } else {
        None
    }
}

fn populate_current_contents(repo: &Repository, repo_path: &Path, export_data: &mut ExportData, silent: bool) -> Result<()> {
    let total_files = export_data.len();
    let pb = if !silent {
        let progress_bar = ProgressBar::new(total_files as u64);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}/{eta_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg} [{per_sec}]")
                .unwrap()
                .progress_chars("##-")
        );
        progress_bar.set_message("Reading current file contents");
        Some(progress_bar)
    } else {
        None
    };
    
    // Get the current HEAD tree to check which files still exist
    let head_tree = if let Ok(head) = repo.head() {
        if let Ok(commit) = head.peel_to_commit() {
            Some(commit.tree()?)
        } else {
            None
        }
    } else {
        None
    };
    
    let mut processed_count = 0;
    let update_interval = std::cmp::max(1, total_files / 100); // Update every 1% of files
    
    for (file_path, file_info) in export_data.iter_mut() {
        // Check if file exists in current HEAD
        let current_contents = if let Some(tree) = &head_tree {
            if let Ok(entry) = tree.get_path(Path::new(file_path)) {
                if let Ok(object) = entry.to_object(repo) {
                    if object.kind() == Some(ObjectType::Blob) {
                        let blob = object.as_blob().unwrap();
                        let content = blob.content();
                        
                        // Quick binary detection - check for null bytes in first 8192 bytes
                        let check_len = std::cmp::min(content.len(), 8192);
                        if content[..check_len].contains(&0) {
                            "[Binary file]".to_string()
                        } else {
                            String::from_utf8_lossy(content).to_string()
                        }
                    } else {
                        "[Binary file or unreadable]".to_string()
                    }
                } else {
                    "[deleted]".to_string()
                }
            } else {
                "[deleted]".to_string()
            }
        } else {
            // No HEAD commit, try to read from filesystem
            let full_path = repo_path.join(file_path);
            if full_path.exists() {
                // Try to detect binary files early
                match fs::read(&full_path) {
                    Ok(content) => {
                        let check_len = std::cmp::min(content.len(), 8192);
                        if content.len() > 0 && content[..check_len].contains(&0) {
                            "[Binary file]".to_string()
                        } else {
                            String::from_utf8_lossy(&content).to_string()
                        }
                    }
                    Err(_) => "[binary file or unreadable]".to_string(),
                }
            } else {
                "[deleted]".to_string()
            }
        };
        
        file_info.current_contents = current_contents;
        
        processed_count += 1;
        // Batch update progress bar for better performance
        if processed_count % update_interval == 0 || processed_count == total_files {
            if let Some(progress_bar) = &pb {
                progress_bar.set_position(processed_count as u64);
            }
        }
    }
    
    if let Some(progress_bar) = pb {
        progress_bar.finish_with_message("Finished reading current file contents");
    }
    Ok(())
}
