
use clap::{Parser};
use anyhow::{Result, Error};
use std::time::Instant;
use std::path::PathBuf;
use rayon::prelude::*;
use mj_io::{build_pbar, expand_dirs, write_mem_to_pathbuf, get_output_filename};
use polars::prelude::*;


/*

Quick'n'dirty rust tool to convert .parquets to jsonl.gz files
Steps:
1. List all .parquet files
2. (in parallel) break into jsonl.zstds's  
3. Writes to output directory 


Notes: will only keep the provided 'text' and 'id' fields
*/


#[derive(Parser, Debug)]
struct Args {

    /// Input dir location 
    #[arg(required=true, long)]
    input_dir: PathBuf,    

    /// Output location 
    #[arg(required=true, long)]
    output_dir: PathBuf,

}

/*====================================================
=                  PROCESS PARQUET                   =
====================================================*/


fn convert_pqt_to_jsonl(input_path: &PathBuf, output_path: &PathBuf) -> Result<(), Error> {
    let df = LazyFrame::scan_parquet(input_path, ScanArgsParquet::default()).unwrap()
        .collect()?;
    // Convert to JSON strings
    let mut output_vec: Vec<u8> = Vec::new();
    df.iter().for_each(|row| {
        let json_row = serde_json::to_vec(&row.iter()
                .zip(df.get_column_names())
                .map(|(value, name)| (name, value))
                .collect::<std::collections::HashMap<_, _>>())
                .unwrap();   
        output_vec.extend(json_row);
        output_vec.push(b'\n');

    });

    write_mem_to_pathbuf(&output_vec, output_path).unwrap();

    Ok(())
}

/*====================================================
=                   MAIN FUNCTION                    =
====================================================*/


fn main() {
    let start_time = Instant::now();
    let args = Args::parse();

    let input_files: Vec<PathBuf> = expand_dirs(vec![args.input_dir.clone()], None).unwrap();
    let num_inputs = input_files.len();


    let pbar = build_pbar(num_inputs, "Parquets");

    input_files.par_iter().for_each(|p| {
        let output_p = get_output_filename(p, &args.input_dir, &args.output_dir).unwrap().with_extension("jsonl.zst");
        convert_pqt_to_jsonl(p, &output_p).unwrap();
        
        pbar.inc(1);
    });


    println!("-------------------------");
    println!("Completed parquet to json in {:?} (s)", start_time.elapsed().as_secs());


}



