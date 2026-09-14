//! CLI tool to promote a replica to primary.
//!
//! Connects to the replica's promotion endpoint, authenticates via
//! Ed25519 challenge-response (operator key required), and sends the
//! PROMOTE command.
//!
//! Usage:
//!   melin-ec-promote <addr> <key-file>
//!
//! Example:
//!   melin-ec-promote 127.0.0.1:9878 ops.key

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::Duration;

use melin_client::{Connection, Error, key};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: melin-ec-promote <addr> <key-file>");
        eprintln!("  addr:     promote endpoint of the replica (e.g. 127.0.0.1:9878)");
        eprintln!("  key-file: path to the Ed25519 operator private key (32-byte seed)");
        eprintln!();
        eprintln!("example:");
        eprintln!("  melin-ec-promote 127.0.0.1:9878 ops.key");
        std::process::exit(1);
    }

    let addr: std::net::SocketAddr = match args[1].parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: invalid address '{}': {e}", args[1]);
            std::process::exit(1);
        }
    };

    // The operator signing key: a raw 32-byte Ed25519 seed, or the PKCS#8
    // PEM `openssl genpkey` writes.
    let signing_key = match key::load_signing_key(Path::new(&args[2])) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

    // The promotion endpoint authenticates exactly like a node — the
    // Ed25519 challenge-response the sequencer's client implements — and
    // then speaks text lines, so the socket is taken back once the
    // handshake is done.
    eprintln!("Connecting to {addr}...");
    let mut connection =
        match Connection::connect_timeout(addr, &signing_key, Duration::from_secs(5)) {
            Ok(c) => c,
            Err(Error::AuthFailed { .. }) => {
                eprintln!(
                    "error: authentication failed — key not authorized or not an operator key"
                );
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        };
    eprintln!("Authenticated.");
    // A promotion can take a moment on a busy replica: longer than the
    // connect and handshake bound.
    if let Err(e) = connection.set_read_timeout(Duration::from_secs(10)) {
        eprintln!("error: failed to set read timeout: {e}");
        std::process::exit(1);
    }
    let mut stream = connection.into_stream();

    // --- Send PROMOTE command ---
    if let Err(e) = stream.write_all(b"PROMOTE\n") {
        eprintln!("error: failed to send PROMOTE: {e}");
        std::process::exit(1);
    }
    stream.flush().expect("flush");

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    match reader.read_line(&mut response) {
        Ok(0) => {
            eprintln!("error: server closed connection without response");
            std::process::exit(1);
        }
        Ok(_) => {
            let trimmed = response.trim();
            if trimmed == "OK" {
                eprintln!("Promotion successful — replica is now primary.");
            } else {
                eprintln!("Promotion failed: {trimmed}");
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("error: failed to read response: {e}");
            std::process::exit(1);
        }
    }
}
