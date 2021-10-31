use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use byte_unit::Byte;
use clap::{crate_version, Parser};
use heed::EnvOpenOptions;
use twitch_irc::login::StaticLoginCredentials;
use twitch_irc::{ClientConfig, SecureTCPTransport, TwitchIRCClient};
use twitch_messages::{Index, TimedUserMessage};

/// The duration to wait before computing the amount of messages seen.
const DURATION_SAMPLE: Duration = Duration::from_secs(10);

#[derive(Debug, Parser)]
#[clap(version = crate_version!(), author = "Kerollmops <kero@meilisearch.com>")]
struct Opts {
    /// The path where the database is located.
    #[clap(long, default_value = "database.tm")]
    database_path: PathBuf,

    /// The maximum size of the database.
    #[clap(long, default_value = "100 GiB")]
    max_db_size: Byte,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[clap(short, long, parse(from_occurrences))]
    verbose: usize,

    #[clap(subcommand)]
    command: SubCommand,
}

#[derive(Debug, Parser)]
enum SubCommand {
    /// Run a subscribe system to fetch messages.
    Subscribe {
        /// The list of channels to fetch.
        channels: Vec<String>,
    },
    /// Import messages from the standard input.
    ///
    /// The format must be a CSV with an UTC timestamp, the channel name,
    /// the login of the user posting the message text.
    ///
    ///     timestamp,channel,login,text
    ImportMessages,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    let mut options = EnvOpenOptions::new();
    options.map_size(opts.max_db_size.get_bytes() as usize);
    let _ = std::fs::create_dir_all(&opts.database_path);
    let (msg_sender, _index) = Index::open(options, opts.database_path)?;

    match opts.command {
        SubCommand::Subscribe { channels } => {
            let config = ClientConfig::new_simple(StaticLoginCredentials::anonymous());
            let (mut incoming_messages, client) =
                TwitchIRCClient::<SecureTCPTransport, _>::new(config);

            for channel in channels {
                client.join(channel);
            }

            let mut start = Instant::now();
            let mut count = 0u64;

            while let Some(message) = incoming_messages.recv().await {
                match TimedUserMessage::from_private_nessage(message) {
                    Some(msg) => {
                        msg_sender.send(msg).unwrap();
                        count += 1;
                    }
                    None => continue,
                }

                if let Some(_) = start.elapsed().checked_sub(DURATION_SAMPLE) {
                    let seconds = DURATION_SAMPLE.as_secs();
                    eprintln!("We see {:.02} msg/s", count as f64 / seconds as f64);
                    start = Instant::now();
                    count = 0;
                }
            }
        }
        SubCommand::ImportMessages => {
            let stdin = io::stdin();
            let mut reader = csv::Reader::from_reader(stdin);

            let headers = reader.headers()?;
            assert_eq!(headers, vec!["timestamp", "channel", "login", "text"]);

            let mut start = Instant::now();
            let mut count = 0u64;

            let mut record = csv::StringRecord::new();
            while reader.read_record(&mut record)? {
                if let Some(msg) = TimedUserMessage::from_string_record(&record) {
                    msg_sender.send(msg).unwrap();
                    count += 1;
                }

                if let Some(_) = start.elapsed().checked_sub(DURATION_SAMPLE) {
                    let seconds = DURATION_SAMPLE.as_secs();
                    eprintln!("We see {:.02} msg/s", count as f64 / seconds as f64);
                    start = Instant::now();
                    count = 0;
                }
            }

            eprintln!("The system is now stabilizing, CTRL-C it when it imported everything");

            std::thread::park();
        }
    }

    Ok(())
}
