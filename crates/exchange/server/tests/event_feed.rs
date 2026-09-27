//! The event feed's handshake, against a real node: the listener behind
//! `--event-bind` admits a client key and refuses a replication key,
//! which authorizes streaming between nodes and nothing else.
//!
//! Which keys the feed admits is unit-tested in `event_publisher`; this
//! checks that the node runs that decision on the handshake a subscriber
//! actually goes through.
//!
//! Trading-only: the skip-order-exec build runs no event publisher.

#![cfg(not(feature = "skip-order-exec"))]

mod common;

use std::net::{SocketAddr, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use base64::Engine;
use common::{free_port, server_bin};
use ed25519_dalek::SigningKey;

/// A node that is killed when the test ends, pass or fail.
struct Node(Child);

impl Drop for Node {
    fn drop(&mut self) {
        // Best-effort: the node may have exited already, and a test that
        // is tearing down has nothing better to do with the error.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn authorized_keys_line(role: &str, key: &SigningKey) -> String {
    let public = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());
    format!("{role} {public} {role}-key\n")
}

/// Connect to the feed, retrying while the node starts. The read timeout
/// bounds a handshake the node never answers.
fn connect(node: &mut Node, addr: SocketAddr) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_secs(1)) {
            Ok(stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .expect("set read timeout");
                return stream;
            }
            Err(e) => {
                if let Some(status) = node.0.try_wait().expect("poll node") {
                    panic!("node exited before its event feed came up: {status}");
                }
                assert!(
                    Instant::now() < deadline,
                    "event feed at {addr} never came up: {e}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

#[test]
fn event_feed_admits_a_client_key_and_refuses_a_replication_key() {
    let readonly = SigningKey::from_bytes(&[0xE1; 32]);
    let replication = SigningKey::from_bytes(&[0xE2; 32]);

    let tmp = tempfile::tempdir().expect("create temp dir");
    let keys_path = tmp.path().join("authorized_keys");
    std::fs::write(
        &keys_path,
        authorized_keys_line("readonly", &readonly)
            + &authorized_keys_line("replication", &replication),
    )
    .expect("write authorized_keys");

    let journal = tmp.path().join("node.journal");
    let event_addr: SocketAddr = format!("127.0.0.1:{}", free_port())
        .parse()
        .expect("event address");
    let mut node = Node(
        Command::new(server_bin())
            .args([
                "--bind",
                &format!("127.0.0.1:{}", free_port()),
                "--health-bind",
                &format!("127.0.0.1:{}", free_port()),
                "--event-bind",
                &event_addr.to_string(),
                "--standalone",
                "--ack-policy",
                "disk",
                "--journal",
                journal.to_str().expect("utf-8 path"),
                "--authorized-keys",
                keys_path.to_str().expect("utf-8 path"),
                "--accounts",
                "1",
                "--instruments",
                "1",
                "--cores",
                "none",
            ])
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn node"),
    );

    // The admitted key goes first: once it is through, the feed is up, so
    // the refusal below is the handshake's verdict and not a node still
    // starting.
    {
        let mut stream = connect(&mut node, event_addr);
        melin_client::authenticate(&mut stream, &readonly).expect("a readonly key subscribes");
    }

    // Listed, and signing the challenge correctly: refused for its role.
    let mut stream = connect(&mut node, event_addr);
    match melin_client::authenticate(&mut stream, &replication) {
        Err(melin_client::Error::AuthFailed { public_key }) => {
            assert_eq!(public_key, replication.verifying_key().to_bytes());
        }
        other => panic!("a replication key must be refused, got {other:?}"),
    }
}
