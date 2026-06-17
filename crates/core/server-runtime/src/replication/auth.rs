//! Replication authentication — Ed25519 challenge/response.
//!
//! Both halves of the handshake live here: `authenticate_replica` runs on
//! the primary side and verifies the replica's signature; `authenticate_with_primary`
//! runs on the replica side and signs the challenge.
//!
//! The wire framing and message encoders/decoders live in
//! `melin_transport_core::replication::protocol`; this module is the
//! exchange-side glue that pairs the generic auth flow with the
//! operator-managed `AuthorizedKeys` permission table.

use std::io::{self, Read, Write};

use melin_transport_core::replication::protocol::{
    MAX_CONTROL_FRAME, decode_auth_result, decode_challenge, decode_challenge_response,
    encode_auth_failed, encode_auth_ok, encode_challenge, encode_challenge_response, read_frame,
};

/// Generate a fresh 32-byte challenge nonce.
///
/// Shared by the blocking [`authenticate_replica`] and the non-blocking DPDK
/// sender state machine so both issue challenges identically.
pub(super) fn generate_challenge_nonce() -> io::Result<[u8; 32]> {
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| io::Error::other(format!("getrandom failed: {e}")))?;
    Ok(nonce)
}

/// Verify a replica's `ChallengeResponse` against the `nonce` we issued and
/// the operator's `AuthorizedKeys`: the key must be listed, carry
/// `Replication` permission, and produce a valid Ed25519 signature over the
/// nonce. `response_frame` is the decoded frame payload (length prefix
/// stripped).
///
/// Pure (no I/O) on purpose — the blocking kernel-TCP path and the
/// non-blocking DPDK poll loop both call this, so the security-critical
/// verification can never diverge between transports.
pub(super) fn verify_challenge_response(
    nonce: &[u8; 32],
    response_frame: &[u8],
    authorized_keys: &melin_app::auth::AuthorizedKeys,
) -> io::Result<()> {
    use ed25519_dalek::{Verifier, VerifyingKey};

    let (signature_bytes, pubkey_bytes) = decode_challenge_response(response_frame)
        .map_err(|e| io::Error::other(format!("bad challenge response: {e}")))?;

    let permission = authorized_keys
        .lookup(&pubkey_bytes)
        .ok_or_else(|| io::Error::other("unknown replication key"))?;
    if !permission.is_replication() {
        return Err(io::Error::other(format!(
            "key has {permission:?} permission, expected Replication"
        )));
    }

    let verifying_key = VerifyingKey::from_bytes(&pubkey_bytes)
        .map_err(|e| io::Error::other(format!("invalid public key: {e}")))?;
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    verifying_key
        .verify(nonce, &signature)
        .map_err(|e| io::Error::other(format!("signature verification failed: {e}")))?;

    Ok(())
}

/// Authenticate a replica connection (primary side, blocking — used by the
/// kernel-TCP sender and tests).
///
/// Sends a 32-byte nonce challenge, verifies the replica's Ed25519
/// signature, and checks that the key has `Replication` permission. Must
/// complete within the stream's existing read timeout. The DPDK sender runs
/// the same exchange non-blocking on its poll loop, reusing
/// [`generate_challenge_nonce`] and [`verify_challenge_response`].
pub(super) fn authenticate_replica<S: Read + Write>(
    stream: &mut S,
    authorized_keys: &melin_app::auth::AuthorizedKeys,
) -> io::Result<()> {
    let nonce = generate_challenge_nonce()?;

    // Send Challenge.
    let mut buf = Vec::with_capacity(64);
    encode_challenge(&nonce, &mut buf);
    stream.write_all(&buf)?;
    stream.flush()?;

    // Read and verify the ChallengeResponse.
    let frame = read_frame(stream, MAX_CONTROL_FRAME)?;
    if let Err(e) = verify_challenge_response(&nonce, &frame, authorized_keys) {
        // Best-effort AuthFailed notice before we bail — the connection is
        // about to drop, so a failed write here is not actionable.
        buf.clear();
        encode_auth_failed(&mut buf);
        let _ = stream.write_all(&buf);
        return Err(e);
    }

    // Auth succeeded.
    buf.clear();
    encode_auth_ok(&mut buf);
    stream.write_all(&buf)?;
    stream.flush()?;

    Ok(())
}

/// Authenticate with the primary (replica side).
///
/// Reads the nonce challenge, signs it with the replica's private key,
/// sends the response, and waits for AuthOk/AuthFailed.
pub(super) fn authenticate_with_primary<S: Read + Write>(
    stream: &mut S,
    signing_key: &ed25519_dalek::SigningKey,
) -> io::Result<()> {
    use ed25519_dalek::Signer;

    // Read Challenge.
    let frame = read_frame(stream, MAX_CONTROL_FRAME)?;
    let nonce = decode_challenge(&frame)?;

    // Sign the nonce.
    let signature = signing_key.sign(&nonce);
    let pubkey = signing_key.verifying_key();

    // Send ChallengeResponse.
    let mut buf = Vec::with_capacity(128);
    encode_challenge_response(&signature.to_bytes(), pubkey.as_bytes(), &mut buf);
    stream.write_all(&buf)?;
    stream.flush()?;

    // Read auth result.
    let result_frame = read_frame(stream, MAX_CONTROL_FRAME)?;
    match decode_auth_result(&result_frame)? {
        true => Ok(()),
        false => Err(io::Error::other("primary rejected replication key")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    /// Build an `AuthorizedKeys` table granting `permission` to `key`.
    fn keys_for(key: &SigningKey, permission: &str) -> melin_app::auth::AuthorizedKeys {
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            key.verifying_key().to_bytes(),
        );
        melin_app::auth::AuthorizedKeys::parse(&format!("{permission} {pub_b64} test\n")).unwrap()
    }

    /// Encode a `ChallengeResponse` and return just the payload (4-byte LE
    /// length prefix stripped — see `protocol::read_frame`), matching what
    /// the runtime feeds to `verify_challenge_response`.
    fn response_payload(key: &SigningKey, nonce: &[u8; 32]) -> Vec<u8> {
        let sig = key.sign(nonce);
        let mut frame = Vec::new();
        melin_transport_core::replication::protocol::encode_challenge_response(
            &sig.to_bytes(),
            key.verifying_key().as_bytes(),
            &mut frame,
        );
        frame[4..].to_vec()
    }

    #[test]
    fn verify_accepts_valid_replication_key() {
        let key = SigningKey::from_bytes(&[0x11; 32]);
        let keys = keys_for(&key, "replication");
        let nonce = [0x42; 32];
        assert!(verify_challenge_response(&nonce, &response_payload(&key, &nonce), &keys).is_ok());
    }

    #[test]
    fn verify_rejects_unknown_key() {
        let signer = SigningKey::from_bytes(&[0x22; 32]);
        let listed = SigningKey::from_bytes(&[0x33; 32]);
        let keys = keys_for(&listed, "replication"); // table lists a different key
        let nonce = [0x42; 32];
        let err = verify_challenge_response(&nonce, &response_payload(&signer, &nonce), &keys)
            .unwrap_err();
        assert!(err.to_string().contains("unknown"));
    }

    #[test]
    fn verify_rejects_non_replication_permission() {
        let key = SigningKey::from_bytes(&[0x44; 32]);
        let keys = keys_for(&key, "trader"); // valid key, wrong permission
        let nonce = [0x42; 32];
        let err =
            verify_challenge_response(&nonce, &response_payload(&key, &nonce), &keys).unwrap_err();
        assert!(err.to_string().contains("Replication"));
    }

    #[test]
    fn verify_rejects_signature_over_wrong_nonce() {
        let key = SigningKey::from_bytes(&[0x55; 32]);
        let keys = keys_for(&key, "replication");
        // Replica signed a different nonce than the one we verify against.
        let signed = [0x01; 32];
        let challenge = [0x02; 32];
        let err = verify_challenge_response(&challenge, &response_payload(&key, &signed), &keys)
            .unwrap_err();
        assert!(err.to_string().contains("signature"));
    }
}
