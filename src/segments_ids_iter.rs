use std::convert::TryInto;

use crate::SegmentId;

pub struct SegmentsIdsIter<'txn> {
    iter: heed::RoIter<'txn>,
}

impl<'txn> SegmentsIdsIter<'txn> {
    pub fn new(iter: heed::RoIter<'txn>) -> SegmentsIdsIter<'txn> {
        SegmentsIdsIter { iter }
    }
}

impl Iterator for SegmentsIdsIter<'_> {
    type Item = anyhow::Result<SegmentId>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((bytes, _))) => match bytes.try_into().map(SegmentId::from_be_bytes) {
                Ok(segment_id) => Some(Ok(segment_id)),
                Err(e) => Some(Err(e.into())),
            },
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }
}
