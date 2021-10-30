use std::path::PathBuf;
use std::time::{Duration, Instant};

use byte_unit::Byte;
use clap::{crate_version, Parser};
use heed::EnvOpenOptions;
use twitch_irc::login::StaticLoginCredentials;
use twitch_irc::{ClientConfig, SecureTCPTransport, TwitchIRCClient};
use twitch_messages::{Index, TimedUserMessage};

// inoxtag gotaga sardoche zerator domingo antoinedaniel ponce fantabobshow joueur_du_grenier alphacast chowh1 locklear jlamaru jltomy maghla alexclick etoiles michou_twitch mynthos lebouseuh jeeltv littlebigwhale angledroit trinity deujna gom4rt camak solary jeanmassiet damdamlive ultia kennystream ntk_tv gobgg rivenzi tpk_live drfeelgood lapi lege xari gius aminematue dach doigby jirayalecochon jlfakemonster kamet0 mistermv moman rebeudeter zevent

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

    /// The list of channels to fetch.
    channels: Vec<String>,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[clap(short, long, parse(from_occurrences))]
    verbose: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    let mut options = EnvOpenOptions::new();
    options.map_size(opts.max_db_size.get_bytes() as usize);
    let _ = std::fs::create_dir_all(&opts.database_path);
    let (msg_sender, _index) = Index::open(options, opts.database_path)?;

    let config = ClientConfig::new_simple(StaticLoginCredentials::anonymous());
    let (mut incoming_messages, client) = TwitchIRCClient::<SecureTCPTransport, _>::new(config);

    for channel in opts.channels {
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

    Ok(())
}
