use std::str;

use chrono::{DateTime, Utc};
use obkv::{KvReaderU8, KvWriterU32, KvWriterU8};
use twitch_irc::message::ServerMessage;

#[derive(Debug)]
pub struct TimedUserMessage {
    pub timestamp: DateTime<Utc>,
    pub channel: String,
    pub login: String,
    pub text: String,
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
    pub channel: &'a str,
    pub login: &'a str,
    pub text: &'a str,
}

impl<'a> UserMessage<'a> {
    const CHANNEL: u8 = 0;
    const LOGIN: u8 = 1;
    const TEXT: u8 = 2;

    pub fn from_obkv(bytes: &'a [u8]) -> UserMessage<'a> {
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

    pub fn into_obkv<'b>(&self, buffer: &'b mut Vec<u8>) -> &'b [u8] {
        let start = buffer.len();
        let mut msg_writer = KvWriterU8::new(buffer);
        msg_writer.insert(UserMessage::CHANNEL, self.channel).unwrap();
        msg_writer.insert(UserMessage::LOGIN, self.login).unwrap();
        msg_writer.insert(UserMessage::TEXT, self.text).unwrap();
        let buffer = msg_writer.into_inner().unwrap();
        &buffer[start..]
    }
}

/// This function generates an obkv inside of another obkv.
pub fn obkv_messages_from_msg<'b>(
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
