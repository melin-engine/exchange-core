//! Ed25519 authentication helpers shared by all gateway binaries.

use ed25519_dalek::SigningKey;

/// Load a 32-byte Ed25519 signing key seed from a file.
///
/// The file must contain exactly 32 raw bytes (the Ed25519 seed).
/// Returns the derived `SigningKey`.
pub fn load_signing_key(path: &std::path::Path) -> Result<SigningKey, Box<dyn std::error::Error>> {
    let seed = std::fs::read(path)?;
    if seed.len() != 32 {
        return Err(format!(
            "key file must be 32 bytes, got {} ({})",
            seed.len(),
            path.display()
        )
        .into());
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&seed);
    Ok(SigningKey::from_bytes(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_valid_key() {
        let dir = std::env::temp_dir().join("gateway_core_test_key");
        std::fs::write(&dir, [0xABu8; 32]).unwrap();
        let key = load_signing_key(&dir).unwrap();
        // Verify the key was constructed from our seed.
        assert_eq!(key.to_bytes(), [0xAB; 32]);
        std::fs::remove_file(&dir).unwrap();
    }

    #[test]
    fn reject_wrong_length() {
        let dir = std::env::temp_dir().join("gateway_core_test_key_bad");
        std::fs::write(&dir, [0u8; 16]).unwrap();
        let err = load_signing_key(&dir).unwrap_err();
        assert!(err.to_string().contains("32 bytes"));
        std::fs::remove_file(&dir).unwrap();
    }

    #[test]
    fn reject_missing_file() {
        let result = load_signing_key(std::path::Path::new("/nonexistent/path"));
        assert!(result.is_err());
    }
}
