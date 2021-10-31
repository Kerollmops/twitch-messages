use std::borrow::Cow;
use std::fs::File;
use std::io::{Cursor, Seek};
use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::RecvTimeoutError;
use fst::IntoStreamer;
use grenad::CompressionType::Snappy;
use heed::{RoTxn, RwTxn};
use obkv::{KvReaderU32, KvWriterU32};
use ordered_float::NotNan;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use synchronoise::event::SignalEvent;
use unicode_segmentation::UnicodeSegmentation;
use unidecode::unidecode;

pub use crate::message::TimedUserMessage;
use crate::message::{obkv_messages_from_msg, UserMessage};
use crate::segments_ids_iter::SegmentsIdsIter;

mod message;
mod segments_ids_iter;

type SegmentId = u32;

/// This represent 1 GiB.
const ONE_GIB: usize = 1 * 1024 * 1024 * 1024;
/// This represent 2 GiB.
const TWO_GIB: usize = 2 * ONE_GIB;

/// Number of messages before hard-flushing.
const FLUSH_MESSAGES_COUNT: usize = 10_000;

/// The number of segments we merge in one batch.
const MERGE_FACTOR: usize = 8;

/// The maximum size of a segment allowed to be merged (2 GiB).
const MERGEABLE_LIMIT: u64 = 2 * 1024 * 1024 * 1024;

const LEVEL_LOG_SPAN: f64 = 0.75;

mod main_keys {
    pub const SEGMENT_ID: &str = "segment-id";
}

mod db_names {
    pub const MAIN: &str = "main";
    pub const MARKER: &str = "marker";
    pub const MESSAGES: &str = "messages";
    pub const TERMS: &str = "terms";
}

#[derive(Clone)]
pub struct Index {
    env: heed::Env,
    main: heed::Database,
    marker: heed::Database,
    messages: heed::Database,
    terms: heed::Database,
    new_segment_notifier: Arc<SignalEvent>,
}

impl Index {
    pub fn open<P: AsRef<Path>>(
        mut options: heed::EnvOpenOptions,
        path: P,
    ) -> heed::Result<(crossbeam_channel::Sender<TimedUserMessage>, Index)> {
        options.max_dbs(4);
        let env = options.open(path)?;
        let main = env.create_database(Some(db_names::MAIN))?;
        let marker = env.create_database(Some(db_names::MARKER))?;
        let messages = env.create_database(Some(db_names::MESSAGES))?;
        let terms = env.create_database(Some(db_names::TERMS))?;
        let new_segment_notifier = Arc::new(SignalEvent::auto(true));

        let index = Index {
            env,
            main,
            marker,
            messages,
            terms,
            new_segment_notifier: new_segment_notifier.clone(),
        };

        let document_sender = start_receiving_task(index.clone());
        let index_cloned = index.clone();
        std::thread::spawn(move || loop {
            new_segment_notifier.wait();
            let _ = index_cloned.clear_stale_reader();
            if let Err(e) = compact_segments(&index_cloned) {
                eprintln!("while trying to compact: {}", e);
            }
        });

        Ok((document_sender, index))
    }

    pub fn write_txn(&self) -> heed::Result<RwTxn> {
        self.env.write_txn()
    }

    pub fn read_txn(&self) -> heed::Result<RoTxn> {
        self.env.read_txn()
    }

    pub fn new_segment_notifier(&self) -> Arc<SignalEvent> {
        self.new_segment_notifier.clone()
    }

    pub fn clear_stale_reader(&self) -> heed::Result<usize> {
        self.env.clear_stale_readers()
    }

    fn increment_segment_id(&self, wtxn: &mut RwTxn) -> anyhow::Result<SegmentId> {
        let new_segment_id = match self.main.get(wtxn, main_keys::SEGMENT_ID)? {
            Some(bytes) => bytes.try_into().map(SegmentId::from_be_bytes)? + 1,
            None => 0,
        };
        self.main.put(wtxn, main_keys::SEGMENT_ID, new_segment_id.to_be_bytes())?;
        Ok(new_segment_id)
    }

    pub fn segment_total_size(&self, rtxn: &RoTxn, segment_id: SegmentId) -> heed::Result<u64> {
        let messages_size = self
            .messages
            .get(rtxn, segment_id.to_be_bytes())
            .map(|bytes| bytes.map_or(0, |b| b.len() as u64))?;

        let terms_size = self
            .terms
            .get(rtxn, segment_id.to_be_bytes())
            .map(|bytes| bytes.map_or(0, |b| b.len() as u64))?;

        Ok(messages_size + terms_size)
    }

    pub fn segments_ids<'txn>(&self, rtxn: &'txn RoTxn) -> heed::Result<SegmentsIdsIter<'txn>> {
        self.marker.iter(rtxn).map(SegmentsIdsIter::new)
    }

    /// Returns the list of segments ids grouped by their levels, the result is only valid
    /// for the given transaction.
    ///
    /// It follows the rules of Lucene: <https://runzhuoli.me/2018/08/07/merge-policies-in-solr.html>.
    fn segments_levels(&self, rtxn: &RoTxn) -> anyhow::Result<Vec<Vec<SegmentId>>> {
        // The timestamp-ordered list of level info value of the mergeable segments,
        // computed from the size of them.
        let mut segments_level_value = Vec::new();
        for result in self.segments_ids(rtxn)? {
            let segment_id = result?;
            let segment_size = self.segment_total_size(rtxn, segment_id)?;
            if segment_size < MERGEABLE_LIMIT {
                let level_value = (segment_size as f64 / MERGE_FACTOR as f64).log10();
                segments_level_value.push((segment_id, level_value));
            }
        }

        let mut segments_levels = Vec::new();
        let mut s = 0;
        while let Some(values) = segments_level_value.get(s..).filter(|s| !s.is_empty()) {
            let max_value = values.iter().map(|(_, lv)| NotNan::new(*lv).unwrap()).max().unwrap();
            let min_value = max_value - LEVEL_LOG_SPAN;

            // The newest segment whose level info value is greater than `min_level_value`.
            let v = values.iter().rposition(|(_, lv)| *lv > *min_value).unwrap();

            // The size of the current level.
            let level_size = v + 1;
            let level_segments_ids = values.iter().take(level_size).map(|(id, _)| *id).collect();
            segments_levels.push(level_segments_ids);

            // Skip the already seen segments.
            s += level_size;
        }

        Ok(segments_levels)
    }

    /// Returns the list of segments to compact, the result is only valid for the given transaction.
    ///
    /// It follows the rules of Lucene: <https://runzhuoli.me/2018/08/07/merge-policies-in-solr.html>.
    fn ranges_of_segments_to_compact(&self, rtxn: &RoTxn) -> anyhow::Result<Vec<Vec<SegmentId>>> {
        let mut ranges_to_compact = Vec::new();
        for level_segments_ids in self.segments_levels(rtxn)? {
            for group in level_segments_ids.chunks_exact(MERGE_FACTOR) {
                ranges_to_compact.push(group.to_owned());
            }
        }
        Ok(ranges_to_compact)
    }

    fn remove_segments(&self, wtxn: &mut RwTxn, range: Vec<SegmentId>) -> anyhow::Result<()> {
        for segment_id in range {
            let segment_id_bytes = segment_id.to_be_bytes();
            self.marker.delete(wtxn, segment_id_bytes)?;
            self.messages.delete(wtxn, segment_id_bytes)?;
            self.terms.delete(wtxn, segment_id_bytes)?;
        }

        Ok(())
    }

    fn replace_segments<A>(
        &self,
        wtxn: &mut RwTxn,
        range: Vec<SegmentId>,
        messages: File,
        terms: fst::Set<A>,
    ) -> anyhow::Result<Option<SegmentId>>
    where
        A: AsRef<[u8]>,
    {
        match range.first().copied() {
            Some(first_segment_id) => {
                self.remove_segments(wtxn, range)?;

                let first_segment_id_bytes = first_segment_id.to_be_bytes();
                self.marker.put(wtxn, first_segment_id_bytes, [])?;

                let messages_bytes = unsafe { memmap2::Mmap::map(&messages)? };
                self.messages.put(wtxn, first_segment_id_bytes, messages_bytes)?;

                let terms_bytes = terms.as_fst().as_bytes();
                self.terms.put(wtxn, first_segment_id_bytes, terms_bytes)?;

                Ok(Some(first_segment_id))
            }
            None => Ok(None),
        }
    }

    pub fn inner_iter<F, E>(&self, rtxn: &RoTxn, mut f: F) -> anyhow::Result<Result<(), E>>
    where
        F: FnMut(u64, UserMessage) -> Result<bool, E>,
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
                    match (f)(timestamp, message) {
                        Ok(must_continue) if !must_continue => return Ok(Ok(())),
                        Ok(_must_continue) => (),
                        Err(e) => return Ok(Err(e)),
                    }
                }
            }
        }

        Ok(Ok(()))
    }
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
        let mut terms_sorter = grenad::Sorter::builder(ignore_value)
            .dump_threshold(ONE_GIB)
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

                        // Extract the unidecoded words form the messages texts.
                        for word in msg.text.unicode_words() {
                            let word = unidecode(word);
                            terms_sorter.insert(word, []).unwrap();
                        }

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

            if must_flush && inserted_msgs != 0 {
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

                let terms_file = tempfile::tempfile().unwrap();
                let mut terms_builder = fst::SetBuilder::new(terms_file).unwrap();
                let new_terms_sorter = grenad::Sorter::builder(ignore_value)
                    .dump_threshold(ONE_GIB)
                    .allow_realloc(false)
                    .build();
                let terms_sorter = mem::replace(&mut terms_sorter, new_terms_sorter);
                let mut terms_iter = terms_sorter.into_stream_merger_iter().unwrap();
                while let Some((word, _)) = terms_iter.next().unwrap() {
                    terms_builder.insert(word).unwrap();
                }
                let terms_file = terms_builder.into_inner().unwrap();
                let terms_bytes = unsafe { memmap2::Mmap::map(&terms_file).unwrap() };

                index.marker.put(&mut wtxn, segment_id_bytes, []).unwrap();
                index.messages.put(&mut wtxn, segment_id_bytes, grenad_messages).unwrap();
                index.terms.put(&mut wtxn, segment_id_bytes, terms_bytes).unwrap();

                inserted_msgs = 0;
                wtxn.commit().unwrap();
                index.new_segment_notifier.signal();
            }
        }
    });

    sender
}

fn compact_segments(index: &Index) -> anyhow::Result<()> {
    let rtxn = index.read_txn()?;
    let ranges = index.ranges_of_segments_to_compact(&rtxn)?;
    drop(rtxn);

    ranges.into_par_iter().try_for_each_with(index.clone(), |index, range| {
        // Merge the messages.
        let rtxn = index.read_txn()?;
        let mut segments_ids = Vec::new();
        let mut messages = Vec::new();
        for segment_id in range.iter().copied() {
            if let Some(message_bytes) = index.messages.get(&rtxn, segment_id.to_be_bytes())? {
                let message_cursor = Cursor::new(message_bytes);
                let reader = grenad::Reader::new(message_cursor)?;
                segments_ids.push(segment_id);
                messages.push(reader);
            }
        }

        let new_file = merge_messages(messages)?;

        // Merge the terms of the messages.
        let mut terms_sets = Vec::new();
        for segment_id in range {
            if let Some(terms_bytes) = index.terms.get(&rtxn, segment_id.to_be_bytes())? {
                let set = fst::Set::new(terms_bytes)?;
                terms_sets.push(set);
            }
        }

        let mut op = fst::set::OpBuilder::new();
        for set in terms_sets.iter() {
            op.push(set.into_stream());
        }

        let terms_file = tempfile::tempfile()?;
        let mut terms_builder = fst::SetBuilder::new(terms_file)?;
        terms_builder.extend_stream(op.r#union())?;
        let terms_file = terms_builder.into_inner()?;
        let merged_bytes = unsafe { memmap2::Mmap::map(&terms_file)? };
        let new_terms = fst::Set::new(merged_bytes)?;

        drop(rtxn);

        let mut wtxn = index.write_txn()?;
        eprintln!("Compacting segments {:?}", segments_ids);
        let segment_id = index.replace_segments(&mut wtxn, segments_ids, new_file, new_terms)?;
        if let Some(segment_id) = segment_id {
            eprintln!("Compacted segment {:?}", segment_id);
            index.new_segment_notifier.signal();
        }
        wtxn.commit()?;

        Ok(())
    })
}

fn merge_messages(messages: Vec<grenad::Reader<Cursor<&[u8]>>>) -> anyhow::Result<std::fs::File> {
    let mut merger_builder = grenad::Merger::builder(multi_message_merge);
    for messages in messages {
        merger_builder.push(messages.into_cursor()?);
    }
    let merger = merger_builder.build();

    let mut grenad_writer = tempfile::tempfile()
        .map(|f| grenad::Writer::builder().compression_type(Snappy).build(f))?;
    merger.write_into_stream_writer(&mut grenad_writer)?;
    let mut file = grenad_writer.into_inner()?;
    file.rewind()?;

    Ok(file)
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

    // Remove the documents that are exactly the same, can happen when you
    // import messages while also being subscribed to channels.
    msg_obkvs.dedup();

    let mut buffer = Vec::new();
    for (i, one_msg_obkvs) in msg_obkvs.into_iter().enumerate() {
        let i = i.try_into().unwrap();
        let mut all_msg_writer = KvWriterU32::new(&mut buffer);
        all_msg_writer.insert(i, one_msg_obkvs)?;
    }

    Ok(Cow::Owned(buffer))
}

fn ignore_value<'a>(_key: &[u8], _values: &[Cow<'a, [u8]>]) -> anyhow::Result<Cow<'a, [u8]>> {
    Ok(Cow::Borrowed(&[]))
}
