use std::io;
use std::path::PathBuf;

use byte_unit::Byte;
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

    #[clap(subcommand)]
    command: SubCommand,
}

#[derive(Debug, Parser)]
enum SubCommand {
    PrintAllMessages,
    PrintAllSegments,
}

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    let options = EnvOpenOptions::new();
    let (_, index) = Index::open(options, opts.database_path)?;

    match opts.command {
        SubCommand::PrintAllMessages => {
            let stdout = io::stdout();
            let mut writer = csv::Writer::from_writer(stdout);
            writer.write_record(&["timestamp", "channel", "login", "text"][..])?;

            let rtxn = index.read_txn()?;
            index.inner_iter(&rtxn, |timestamp, msg| {
                let timestamp = timestamp.to_string();
                writer.write_record(&[timestamp.as_str(), msg.channel, msg.login, msg.text][..])?;
                Ok(true) as io::Result<_>
            })??;

            writer.flush()?;
        }
        SubCommand::PrintAllSegments => {
            let rtxn = index.read_txn()?;
            for result in index.segments_ids(&rtxn)? {
                let segment_id = result?;
                let byte_size = index.segment_total_size(&rtxn, segment_id)?;
                let size = Byte::from_bytes(byte_size).get_appropriate_unit(true).to_string();
                println!("{:8} - {:>12}", segment_id, size);
            }
        }
    }

    Ok(())
}
