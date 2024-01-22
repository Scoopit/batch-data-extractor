use std::{
    fs::{self, File},
    time::Instant,
};

use clap::Parser;
use color_eyre::eyre::{Context, ContextCompat};
use datafusion::execution::context::{SessionConfig, SessionContext};
use futures::StreamExt;
use humantime::format_duration;
use parquet_to_mysql::record_batch_to_sql_inserts;
use serde::Deserialize;

use crate::utils::{parse_filename, register_table, sanitize_table_name};

mod utils;

#[derive(Parser)]
struct Opts {
    /// Directory containing files used as data tables
    #[arg(long = "tables", short, default_value = ".")]
    tables_dir: String,

    /// Print out mysqldump connection setup headers & footers (tz, encoding)
    #[arg(long = "header-and-footer", short = 'm')]
    print_mysqldump_header_and_footer: bool,

    /// Yaml file describing batch operation to do on the data
    batch_config: String,
}

#[derive(Deserialize)]
struct Batch {
    queries: Vec<Query>,
}

#[derive(Deserialize)]
struct Query {
    name: String,
    sql: String,
    /// 100 by default
    rows_batch_size: Option<usize>,
    /// If not specified, output table will use the "name" of this query
    table_name: Option<String>,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let opts = Opts::parse();
    let paths = fs::read_dir(&opts.tables_dir)?;
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    for path in paths {
        let path = path?.path();
        let file_name = path
            .file_stem()
            .unwrap()
            .to_str()
            .context("Invalid filename")?;
        let table_name = sanitize_table_name(file_name);
        register_table(&ctx, &table_name, parse_filename(&path)?).await?;
    }
    let batch: Batch =
        serde_yaml::from_reader(File::open(&opts.batch_config).context("Unable to open file")?)
            .context("Unable to parse file")?;

    if opts.print_mysqldump_header_and_footer {
        println!("{}", parquet_to_mysql::MYSQLDUMP_HEADER);
    }

    for query in batch.queries {
        let started = Instant::now();
        let mut rows_count = 0;
        println!("-- Query: {}", query.name);
        if atty::isnt(atty::Stream::Stdout) {
            eprintln!("Executing query {}\n{}", query.name, query.sql);
        }
        let df = ctx.sql(&query.sql).await?;
        let mut stream = df.execute_stream().await?;
        while let Some(record_batch) = stream.next().await {
            let record_batch = record_batch?;
            rows_count += record_batch.num_rows();
            println!(
                "{}",
                record_batch_to_sql_inserts(
                    record_batch,
                    query
                        .table_name
                        .as_ref()
                        .map(String::as_str)
                        .unwrap_or(&query.name),
                    None,
                    query.rows_batch_size.unwrap_or(100),
                )
            );
        }
        println!(
            "-- query {} executed in {} - {rows_count} rows exported!\n",
            query.name,
            format_duration(started.elapsed())
        );
        if atty::isnt(atty::Stream::Stdout) {
            eprintln!(
                "Query {} executed in {} - {rows_count} rows exported!",
                query.name,
                format_duration(started.elapsed())
            );
        }
    }

    if opts.print_mysqldump_header_and_footer {
        println!("{}", parquet_to_mysql::MYSQLDUMP_FOOTER);
    }

    Ok(())
}
