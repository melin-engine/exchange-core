//! Read a journal file and print its final sequence number and BLAKE3 chain hash.
//!
//! Usage: cargo run --release -p melin-server --bin journal-verify -- <path>

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: journal-verify <journal-path>");
    let mut reader =
        melin_journal::JournalReader::<melin_trading::trading_event::TradingEvent>::open(
            path.as_ref(),
        )
        .expect("open journal");

    let mut count = 0u64;
    let mut last_seq = 0u64;
    loop {
        match reader.next_entry() {
            Ok(Some(entry)) => {
                count += 1;
                last_seq = entry.sequence;
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!("error at entry {}: {e}", count + 1);
                break;
            }
        }
    }

    println!("entries:    {count}");
    println!("start_seq:  {}", reader.starting_sequence());
    println!("last_seq:   {last_seq}");
    let hex = |h: [u8; 32]| h.iter().map(|b| format!("{b:02x}")).collect::<String>();
    match reader.anchor() {
        Some(a) => println!("anchor:     {}", hex(a)),
        None => println!("anchor:     (hash-chain disabled in this build)"),
    }
    // For a sealed segment, this value must equal the next segment's
    // header anchor — that pairing is the cross-segment tamper check.
    match reader.chain_hash() {
        Some(h) => println!("chain_hash: {}", hex(h)),
        None => println!("chain_hash: (hash-chain disabled in this build)"),
    }
}
