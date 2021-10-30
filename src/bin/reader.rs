use std::path::PathBuf;

use clap::{crate_version, Parser};
use heed::EnvOpenOptions;
use twitch_messages::Index;

#[derive(Debug, Parser)]
#[clap(version = crate_version!(), author = "Kerollmops <kero@meilisearch.com>")]
struct Opts {
    /// The path where the database is located.
    #[clap(long, default_value = "database.tm")]
    database_path: PathBuf,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[clap(short, long, parse(from_occurrences))]
    verbose: usize,
}

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    let options = EnvOpenOptions::new();
    let (_, index) = Index::open(options, opts.database_path)?;

    let rtxn = index.read_txn()?;
    index.inner_iter(&rtxn, |timestamp, msg| {
        println!("{} - {:?}", timestamp, msg);
        true
    })?;

    Ok(())
}
