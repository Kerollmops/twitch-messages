use std::borrow::Cow;
use std::io::Cursor;
use std::path::Path;
use std::time::Duration;
use std::{mem, str};

use chrono::{DateTime, Utc};
use crossbeam_channel::RecvTimeoutError;
use grenad::CompressionType::Snappy;
use obkv::{KvReaderU32, KvReaderU8, KvWriterU32, KvWriterU8};
use twitch_irc::message::ServerMessage;

type SegmentId = u32;

/// This represent 2 GiB.
const TWO_GIB: usize = 2 * 1024 * 1024 * 1024;
/// Number of messages before hard-flushing.
const FLUSH_MESSAGES_COUNT: usize = 1000;

mod main_keys {
    pub const SEGMENT_ID: &str = "segment-id";
}

mod db_names {
    pub const MAIN: &str = "main";
    pub const MARKER: &str = "marker";
    pub const MESSAGES: &str = "messages";
}

fn start_receiving_task(index: Index) -> crossbeam_channel::Sender<TimedUserMessage> {
    let (sender, receiver) = crossbeam_channel::unbounded::<TimedUserMessage>();

    std::thread::spawn(move || {
        let mut one_msg_buffer = Vec::new();
        let mut all_msg_buffer = Vec::new();
        let mut inserted_msgs = 0usize;
        let mut messages_sorter = grenad::Sorter::builder(multi_message_merge)
            .dump_threshold(TWO_GIB)
            .allow_realloc(false)
            .build();
        let mut must_continue = true;

        while must_continue {
            let must_flush = match receiver.recv_timeout(Duration::from_secs(10)) {
                Ok(timed_msg) => {
                    let timestamp = timed_msg.timestamp.timestamp();
                    // We ignore timestamp below 0.
                    if let Ok(timestamp) = TryInto::<u64>::try_into(timestamp) {
                        let msg = timed_msg.user_message();
                        one_msg_buffer.clear();
                        all_msg_buffer.clear();
                        let obkv =
                            obkv_messages_from_msg(&msg, &mut one_msg_buffer, &mut all_msg_buffer)
                                .unwrap();
                        messages_sorter.insert(timestamp.to_be_bytes(), obkv).unwrap();
                        inserted_msgs += 1;
                    }

                    inserted_msgs >= FLUSH_MESSAGES_COUNT
                }
                Err(RecvTimeoutError::Timeout) => true,
                Err(RecvTimeoutError::Disconnected) => {
                    must_continue = false;
                    true
                }
            };

            if must_flush {
                println!("We are currently flushing {} messages!", inserted_msgs);

                let mut wtxn = index.write_txn().unwrap();
                let segment_id = index.increment_segment_id(&mut wtxn).unwrap();
                let segment_id_bytes = segment_id.to_be_bytes();

                let new_messages_sorter = grenad::Sorter::builder(multi_message_merge)
                    .dump_threshold(TWO_GIB)
                    .allow_realloc(false)
                    .build();
                let mut grenad_writer = tempfile::tempfile()
                    .map(|f| grenad::Writer::builder().compression_type(Snappy).build(f))
                    .unwrap();
                let messages_sorter = mem::replace(&mut messages_sorter, new_messages_sorter);
                messages_sorter.write_into_stream_writer(&mut grenad_writer).unwrap();
                let grenad_messages_file = grenad_writer.into_inner().unwrap();
                let grenad_messages = unsafe { memmap2::Mmap::map(&grenad_messages_file).unwrap() };

                index.marker.put(&mut wtxn, segment_id_bytes, []).unwrap();
                index.messages.put(&mut wtxn, segment_id_bytes, grenad_messages).unwrap();

                inserted_msgs = 0;
                wtxn.commit().unwrap();
            }
        }
    });

    sender
}

#[derive(Clone)]
pub struct Index {
    env: heed::Env,
    main: heed::Database,
    marker: heed::Database,
    messages: heed::Database,
}

impl Index {
    pub fn open<P: AsRef<Path>>(
        mut options: heed::EnvOpenOptions,
        path: P,
    ) -> heed::Result<(crossbeam_channel::Sender<TimedUserMessage>, Index)> {
        options.max_dbs(3);
        let env = options.open(path)?;
        let main = env.create_database(Some(db_names::MAIN))?;
        let marker = env.create_database(Some(db_names::MARKER))?;
        let messages = env.create_database(Some(db_names::MESSAGES))?;
        let index = Index { env, main, marker, messages };
        let document_sender = start_receiving_task(index.clone());
        Ok((document_sender, index))
    }

    pub fn write_txn(&self) -> heed::Result<heed::RwTxn> {
        self.env.write_txn()
    }

    pub fn read_txn(&self) -> heed::Result<heed::RoTxn> {
        self.env.read_txn()
    }

    fn increment_segment_id(&self, wtxn: &mut heed::RwTxn) -> anyhow::Result<SegmentId> {
        let new_segment_id = match self.main.get(wtxn, main_keys::SEGMENT_ID)? {
            Some(bytes) => bytes.try_into().map(SegmentId::from_be_bytes)? + 1,
            None => 0,
        };
        self.main.put(wtxn, main_keys::SEGMENT_ID, new_segment_id.to_be_bytes())?;
        Ok(new_segment_id)
    }

    pub fn inner_iter<F>(&self, rtxn: &heed::RoTxn, mut f: F) -> anyhow::Result<()>
    where
        F: FnMut(u64, UserMessage) -> bool,
    {
        for result in self.messages.iter(rtxn)? {
            let (_segment_id, grenad_bytes) = result?;

            let bytes_cursor = Cursor::new(grenad_bytes);
            let mut grenad_cursor = grenad::Reader::new(bytes_cursor)?.into_cursor()?;
            while let Some((timestamp_bytes, same_msgs_obkvs)) = grenad_cursor.move_on_next()? {
                let timestamp = timestamp_bytes.try_into().map(u64::from_be_bytes)?;
                let messages_obkv = KvReaderU32::new(same_msgs_obkvs);
                for (_, message_bytes) in messages_obkv.iter() {
                    let message = UserMessage::from_obkv(message_bytes);
                    if !(f)(timestamp, message) {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }
}

/// Merges messages that are at the same position (timestamp) in an obkv that regroups them all,
/// ordered by the fields of one message (channel, login, text).
fn multi_message_merge<'a>(_key: &[u8], values: &[Cow<'a, [u8]>]) -> anyhow::Result<Cow<'a, [u8]>> {
    // If there only is one value, just return it.
    if let [value] = values {
        return Ok(value.clone());
    }

    let mut msg_obkvs = Vec::new();
    for all_msg_obkv_bytes in values {
        let all_msg_obkv = KvReaderU32::new(all_msg_obkv_bytes);
        for (_, one_msg_obkv) in all_msg_obkv.iter() {
            msg_obkvs.push(one_msg_obkv);
        }
    }

    // Sorts the messages by channel, login and then text.
    msg_obkvs.sort_unstable_by(|a, b| {
        let a = UserMessage::from_obkv(a);
        let b = UserMessage::from_obkv(b);
        a.cmp(&b)
    });

    let mut buffer = Vec::new();
    for (i, one_msg_obkvs) in msg_obkvs.into_iter().enumerate() {
        let i = i.try_into().unwrap();
        let mut all_msg_writer = KvWriterU32::new(&mut buffer);
        all_msg_writer.insert(i, one_msg_obkvs)?;
    }

    Ok(Cow::Owned(buffer))
}

/// This function generates an obkv inside of another obkv.
fn obkv_messages_from_msg<'b>(
    msg: &UserMessage,
    one_msg_buffer: &mut Vec<u8>,
    all_msg_buffer: &'b mut Vec<u8>,
) -> anyhow::Result<&'b [u8]> {
    let start_buffer = all_msg_buffer.len();
    let msg_bytes = msg.into_obkv(one_msg_buffer);
    let mut all_msg_writer = KvWriterU32::new(all_msg_buffer);
    all_msg_writer.insert(0, msg_bytes)?;
    let all_msg_buffer = all_msg_writer.into_inner()?;
    Ok(&all_msg_buffer[start_buffer..])
}

#[derive(Debug)]
pub struct TimedUserMessage {
    timestamp: DateTime<Utc>,
    channel: String,
    login: String,
    text: String,
}

impl TimedUserMessage {
    pub fn from_private_nessage(msg: ServerMessage) -> Option<TimedUserMessage> {
        if let ServerMessage::Privmsg(msg) = msg {
            Some(TimedUserMessage {
                timestamp: msg.server_timestamp,
                channel: msg.channel_login,
                login: msg.sender.login,
                text: msg.message_text,
            })
        } else {
            None
        }
    }

    pub fn user_message(&self) -> UserMessage {
        UserMessage {
            channel: self.channel.as_str(),
            login: self.login.as_str(),
            text: self.text.as_str(),
        }
    }
}

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct UserMessage<'a> {
    channel: &'a str,
    login: &'a str,
    text: &'a str,
}

impl<'a> UserMessage<'a> {
    const CHANNEL: u8 = 0;
    const LOGIN: u8 = 1;
    const TEXT: u8 = 2;

    fn from_obkv(bytes: &'a [u8]) -> UserMessage<'a> {
        let obkv = KvReaderU8::new(bytes);
        let channel = obkv.get(UserMessage::CHANNEL).unwrap();
        let login = obkv.get(UserMessage::LOGIN).unwrap();
        let text = obkv.get(UserMessage::TEXT).unwrap();
        UserMessage {
            channel: str::from_utf8(channel).unwrap(),
            login: str::from_utf8(login).unwrap(),
            text: str::from_utf8(text).unwrap(),
        }
    }

    fn into_obkv<'b>(&self, buffer: &'b mut Vec<u8>) -> &'b [u8] {
        let start = buffer.len();
        let mut msg_writer = KvWriterU8::new(&mut *buffer);
        msg_writer.insert(UserMessage::CHANNEL, self.channel).unwrap();
        msg_writer.insert(UserMessage::LOGIN, self.login).unwrap();
        msg_writer.insert(UserMessage::TEXT, self.text).unwrap();
        msg_writer.finish().unwrap();
        &buffer[start..]
    }
}
