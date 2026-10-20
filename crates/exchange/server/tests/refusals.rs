//! A request the node refuses is answered, against a real node: a key
//! whose role may not send a request gets a refusal saying so, where the
//! node used to drop the request and leave the client to its timeout, and
//! the connection goes on serving.
//!
//! Which requests each role may send is unit-tested in `request_decoder`;
//! this checks the refusal reaches the exchange's client.
//!
//! Trading-only: connecting queries the engine's request-sequence mark.

#![cfg(not(feature = "skip-order-exec"))]

mod common;

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use base64::Engine;
use common::{free_port, server_bin};
use ed25519_dalek::SigningKey;
use melin_ec_client::{Client, ClientError, RefusalKind};

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

/// Connect, retrying while the node starts.
fn connect(node: &mut Node, addr: SocketAddr, key: &SigningKey) -> Client {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match Client::connect_with_timeout(addr, key, Duration::from_secs(5)) {
            Ok(client) => return client,
            Err(e) => {
                if let Some(status) = node.0.try_wait().expect("poll node") {
                    panic!("node exited before it served: {status}");
                }
                assert!(
                    Instant::now() < deadline,
                    "node at {addr} never served: {e}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

#[test]
fn a_request_the_key_may_not_send_is_refused_and_the_connection_kept() {
    let trader = SigningKey::from_bytes(&[0xF1; 32]);

    let tmp = tempfile::tempdir().expect("create temp dir");
    let keys_path = tmp.path().join("authorized_keys");
    let public =
        base64::engine::general_purpose::STANDARD.encode(trader.verifying_key().to_bytes());
    std::fs::write(&keys_path, format!("trader {public} trader-key\n"))
        .expect("write authorized_keys");

    let journal = tmp.path().join("node.journal");
    let addr: SocketAddr = format!("127.0.0.1:{}", free_port())
        .parse()
        .expect("client address");
    let mut node = Node(
        Command::new(server_bin())
            .args([
                "--bind",
                &addr.to_string(),
                "--health-bind",
                &format!("127.0.0.1:{}", free_port()),
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

    let mut client = connect(&mut node, addr, &trader);
    // Bounds a refusal that never comes, which is what this used to be.
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set read timeout");

    // Stats are the operator's: a trader key is refused, with a reason.
    match client.query_stats() {
        Err(ClientError::Refused { kind, reason }) => {
            assert_eq!(kind, RefusalKind::PermissionDenied);
            assert!(reason.contains("operator"), "{reason}");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }

    // The connection still serves what the key may send.
    client
        .synchronize_request_seq()
        .expect("a trader key reads its request-sequence mark");
}
