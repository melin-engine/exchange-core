//! Generate an Ed25519 keypair for trading engine authentication.
//!
//! Writes:
//!   <name>.key          — 32-byte raw private key seed
//!   <name>.pub          — base64-encoded public key (for authorized_keys file)
//!
//! and prints the `authorized_keys` line for the key.
//!
//! Usage:
//!     melin-ec-keygen <name> <role>
//!
//! Example:
//!     melin-ec-keygen ops operator
//!     melin-ec-keygen market-maker trader
//!     melin-ec-keygen monitor readonly
//!
//! The line is checked with the node's own keys-file parser before
//! anything is written, so a line printed here always loads.

use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::SigningKey;
use melin_app::auth::{AuthorizedKeys, Role};
use melin_ec_protocol::role::ExchangeRole;

/// The node runtime's own roles, accepted beside the exchange's. The
/// runtime does not export its tokens, so they are listed here for the
/// usage text only: the parser in [`authorized_keys_line`] is what
/// decides, so a stale entry could make the help wrong, never admit a bad
/// line.
const RUNTIME_ROLES: [&str; 2] = ["operator", "replication"];

/// Every role a node accepts, for the usage text.
fn role_list() -> String {
    RUNTIME_ROLES
        .into_iter()
        .chain(ExchangeRole::ROLES.iter().map(|&(token, _)| token))
        .collect::<Vec<_>>()
        .join(" | ")
}

/// The `authorized_keys` line listing `public_key_b64` under `role`, with
/// `name` as its comment. Refused, with the node's own error, if a node
/// would refuse to load it.
fn authorized_keys_line(role: &str, public_key_b64: &str, name: &str) -> Result<String, String> {
    let line = format!("{role} {public_key_b64} {name}");
    AuthorizedKeys::parse::<ExchangeRole>(&line)?;
    Ok(line)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: melin-ec-keygen <name> <role>");
        eprintln!("  role: {}", role_list());
        eprintln!();
        eprintln!("example:");
        eprintln!("  melin-ec-keygen ops operator");
        eprintln!("  melin-ec-keygen market-maker trader");
        eprintln!("  melin-ec-keygen treasury custodian");
        std::process::exit(1);
    }

    let name = &args[1];
    let role = &args[2];

    let key_path = format!("{name}.key");
    let pub_path = format!("{name}.pub");

    if Path::new(&key_path).exists() {
        eprintln!("error: {key_path} already exists (refusing to overwrite)");
        std::process::exit(1);
    }

    // Generate random keypair.
    let mut seed = [0u8; 32];
    rand::fill(&mut seed);
    let signing_key = SigningKey::from_bytes(&seed);
    let public_key = signing_key.verifying_key();
    let pub_b64 = BASE64.encode(public_key.as_bytes());

    // Checked before anything is written: a refused role leaves no files.
    let auth_line = authorized_keys_line(role, &pub_b64, name).unwrap_or_else(|e| {
        eprintln!("error: a node would refuse this key's line: {e}");
        std::process::exit(1);
    });

    // Write private key (raw 32-byte seed).
    std::fs::write(&key_path, seed).unwrap_or_else(|e| {
        eprintln!("error writing {key_path}: {e}");
        std::process::exit(1);
    });

    // Write public key (base64).
    std::fs::write(&pub_path, format!("{pub_b64}\n")).unwrap_or_else(|e| {
        eprintln!("error writing {pub_path}: {e}");
        std::process::exit(1);
    });

    // Print authorized_keys line to stdout for easy appending.
    println!("Generated keypair:");
    println!("  Private key: {key_path}");
    println!("  Public key:  {pub_path}");
    println!();
    println!("Add this line to your authorized_keys file:");
    println!("  {auth_line}");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A listed key: 32 zero bytes, base64.
    const KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    /// Every role the usage text offers is one a node loads, so the list
    /// and the node cannot disagree.
    #[test]
    fn every_listed_role_makes_a_line_the_node_loads() {
        for role in role_list().split(" | ") {
            let line = authorized_keys_line(role, KEY, "desk").unwrap();
            assert_eq!(line, format!("{role} {KEY} desk"));
        }
        assert_eq!(
            role_list(),
            "operator | replication | trader | custodian | readonly"
        );
    }

    /// A role the node does not know is refused with the node's own
    /// error, which lists the valid ones; matching is exact.
    #[test]
    fn an_unknown_role_is_refused_with_the_nodes_error() {
        for role in ["admin", "Trader"] {
            let err = authorized_keys_line(role, KEY, "desk").unwrap_err();
            assert!(err.contains(&format!("unknown role '{role}'")), "{err}");
            assert!(
                err.contains("operator, replication, trader, custodian, readonly"),
                "{err}"
            );
        }
    }
}
