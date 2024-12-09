
use indicatif::{ProgressBar, ProgressStyle};
use clap::{Parser};
use anyhow::{Result, Error};
use std::time::Instant;
use std::path::PathBuf;
use rayon::prelude::*;
use crate::local_io::{expand_dirs, write_mem_to_pathbuf, parquet_to_json};
use std::sync::atomic::{AtomicUsize, Ordering};


pub mod local_io;



/*

Quick'n'dirty rust tool to convert .parquets to jsonl.gz files
Steps:
1. List all .parquet files
2. (in parallel) break into jsonl.gz's  
3. Writes to output directory 


Notes: will only keep the provided 'text' and 'id' fields
*/


#[derive(Parser, Debug)]
struct Args {
    /// (List of) directories/files (on s3 or local) that are jsonl.gz or jsonl.zstd files
    #[arg(required=true, long, num_args=1..)]
    input: Vec<PathBuf>,


    /// Output location (may be an s3 uri)
    #[arg(required=true, long)]
    output: PathBuf,

    /// Shard prefix: Files will be named {shard_prefix}_{shard_num}.jsonl.gz
    #[arg(required=true, long)]
    prefix: String,

    /// Name of the field of the text
    #[arg(long, default_value_t=String::from("text"))]
    text_key: String,

    /// Name of the field of the id 
    #[arg(required=true, long)]
    id_key: String,

    /// Maximum docs per jsonl
    #[arg(long, required=true)]
    max_docs: usize,



}

fn build_pbar(num_items: usize, units: &str) -> ProgressBar {
    let mut template = String::from(units);
    template.push_str(" {human_pos}/{human_len} [{elapsed_precise}/{duration_precise}] [{wide_bar:.cyan/blue}]");
    let pbar = ProgressBar::new(num_items as u64)
        .with_style(
            ProgressStyle::with_template(&template).unwrap()
        );
    pbar.inc(0);
    pbar
}




/*====================================================
=                  PROCESS PARQUET                   =
====================================================*/


fn process_parquet(parquet: &PathBuf, counter: &AtomicUsize, text_key: String, id_key: String, max_docs: usize, output_dir: &PathBuf, prefix: &str) -> Result<(), Error> {

    let cols: Vec<String> = vec![text_key, id_key];
    let json_rows = parquet_to_json(parquet, Some(cols)).unwrap();

    for chunk in json_rows.chunks(max_docs) {
        let file_id = counter.fetch_add(1, Ordering::SeqCst);
        let output_filename = get_output_filename(output_dir, prefix, file_id);
        let line_chunk: Vec<String> = chunk.into_iter().map(|el| serde_json::to_string(el).unwrap()).collect();
        let contents = line_chunk.join("\n").into_bytes();
        write_mem_to_pathbuf(&contents, &output_filename).unwrap()
    }


    Ok(())
}


fn get_output_filename(output_dir: &PathBuf, prefix: &str, id: usize) -> PathBuf {
    PathBuf::from(output_dir).join(format!("{}{:08}.jsonl.gz", prefix, id))
}



/*====================================================
=                   MAIN FUNCTION                    =
====================================================*/


fn main() {
    let start_time = Instant::now();
    let args = Args::parse();

    let input_files: Vec<PathBuf> = expand_dirs(args.input).unwrap();
    let num_inputs = input_files.len();

    let pbar = build_pbar(num_inputs, "Parquets");

    let counter = AtomicUsize::new(0);

    input_files.par_iter().for_each(|p| {
        process_parquet(p, &counter, args.text_key.clone(), args.id_key.clone(), args.max_docs, &args.output, &args.prefix).unwrap();
        pbar.inc(1);

    });

    println!("-------------------------");
    println!("Completed parquet to json in {:?} (s)", start_time.elapsed().as_secs());
    println!("Processed {:?} parquet files", num_inputs);
    println!("Created {:?} jsons", counter.fetch_add(0, Ordering::SeqCst));

}



