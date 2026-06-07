//! Compare two journal lineages entry by entry and report divergences.
//!
//! Walks both lineages (archived segments in order, then the live
//! segment — or a single segment when pointed at one), aligns entries
//! by sequence number, and prints the first differing entries with
//! full field detail. Complements `journal-verify`: the verifier
//! proves each lineage is internally dense and chain-consistent, this
//! tool explains *why* two nodes' tail chain hashes disagree.
//!
//! Usage: journal-diff <journal-a> <journal-b> [max-diffs]
//!
//! Exit code 0 = overlap identical; 1 = divergence found.

use std::path::{Path, PathBuf};

use melin_journal::reader::{JournalEntry, JournalReader};
use melin_trading::trading_event::TradingEvent;

/// Sequential reader over a lineage: archives oldest-first, then live.
struct LineageWalker {
    segments: Vec<PathBuf>,
    next_segment: usize,
    reader: Option<JournalReader<TradingEvent>>,
}

impl LineageWalker {
    fn open(path: &Path) -> Self {
        let mut segments: Vec<PathBuf> = melin_journal::segment::list_archives(path)
            .expect("list archives")
            .into_iter()
            .map(|(_, p)| p)
            .collect();
        if path.exists() {
            segments.push(path.to_path_buf());
        }
        assert!(!segments.is_empty(), "no journal segments at {path:?}");
        Self {
            segments,
            next_segment: 0,
            reader: None,
        }
    }

    fn next(&mut self) -> Option<JournalEntry<TradingEvent>> {
        loop {
            if self.reader.is_none() {
                if self.next_segment >= self.segments.len() {
                    return None;
                }
                let p = &self.segments[self.next_segment];
                self.next_segment += 1;
                self.reader =
                    Some(JournalReader::open(p).unwrap_or_else(|e| panic!("open {p:?}: {e}")));
            }
            match self
                .reader
                .as_mut()
                .expect("reader opened above")
                .next_entry()
            {
                Ok(Some(entry)) => return Some(entry),
                Ok(None) => self.reader = None, // segment exhausted — advance
                Err(e) => panic!("read error in segment {}: {e}", self.next_segment - 1),
            }
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let path_a: PathBuf = args.next().expect("usage: journal-diff <a> <b>").into();
    let path_b: PathBuf = args.next().expect("usage: journal-diff <a> <b>").into();
    let max_diffs: usize = args.next().map_or(5, |s| s.parse().expect("max-diffs"));

    let mut a = LineageWalker::open(&path_a);
    let mut b = LineageWalker::open(&path_b);

    let mut ea = a.next();
    let mut eb = b.next();
    let mut compared = 0u64;
    let mut diffs = 0usize;
    let mut first_seq = None;
    let mut last_seq = 0u64;

    while let (Some(x), Some(y)) = (&ea, &eb) {
        // Lineages may start at different sequences (trimmed history);
        // skip the side that is behind until both are aligned.
        if x.sequence < y.sequence {
            ea = a.next();
            continue;
        }
        if y.sequence < x.sequence {
            eb = b.next();
            continue;
        }

        first_seq.get_or_insert(x.sequence);
        last_seq = x.sequence;
        compared += 1;
        if x != y {
            diffs += 1;
            println!("DIFF at sequence {}:", x.sequence);
            if x.timestamp_ns != y.timestamp_ns {
                println!("  timestamp_ns: a={} b={}", x.timestamp_ns, y.timestamp_ns);
            }
            if x.key_hash != y.key_hash {
                println!("  key_hash:     a={:#x} b={:#x}", x.key_hash, y.key_hash);
            }
            if x.request_seq != y.request_seq {
                println!("  request_seq:  a={} b={}", x.request_seq, y.request_seq);
            }
            if x.event != y.event {
                println!("  event a: {:?}", x.event);
                println!("  event b: {:?}", y.event);
            }
            if diffs >= max_diffs {
                println!("(stopping after {max_diffs} diffs)");
                break;
            }
        }
        ea = a.next();
        eb = b.next();
    }

    let range = match first_seq {
        Some(f) => format!("{f}..={last_seq}"),
        None => "(no overlap)".into(),
    };
    println!("compared {compared} entries over {range}; {diffs} differed");
    std::process::exit(if diffs > 0 { 1 } else { 0 });
}
