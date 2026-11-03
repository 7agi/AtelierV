#![allow(dead_code)]
//! Password hashing shared by the server and both admin tools.
//!
//! Stored format: `sha256$<salt hex>$<digest hex>`
//! The digest is SHA-256 over (salt + password), re-hashed ROUNDS times, with a fresh random
//! 16-byte salt per password so identical passwords never produce identical hashes.

use rand::RngCore;
use sha2::{Digest, Sha256};

const PREFIX: &str = "sha256$";
const ROUNDS: u32 = 10_000;

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn digest(salt: &str, password: &str) -> String {
    let mut h = Sha256::new();
    h.update(salt.as_bytes());
    h.update(password.as_bytes());
    let mut out = h.finalize();
    for _ in 1..ROUNDS {
        let mut h = Sha256::new();
        h.update(out);
        h.update(salt.as_bytes());
        h.update(password.as_bytes());
        out = h.finalize();
    }
    to_hex(&out)
}

/// Constant-time byte comparison (avoids leaking how many leading bytes matched).
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Hash a plaintext password into the stored `sha256$salt$digest` form.
pub fn hash_password(password: &str) -> String {
    let mut salt_bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut salt_bytes);
    let salt = to_hex(&salt_bytes);
    format!("{}{}${}", PREFIX, salt, digest(&salt, password))
}

/// True if `stored` is already in the hashed format (as opposed to a legacy plaintext value).
pub fn is_hashed(stored: &str) -> bool {
    match stored.strip_prefix(PREFIX) {
        Some(rest) => rest.matches('$').count() == 1,
        None => false,
    }
}

/// Check a plaintext password against a stored hash.
pub fn verify_password(stored: &str, password: &str) -> bool {
    match stored.strip_prefix(PREFIX).and_then(|r| r.split_once('$')) {
        Some((salt, expected)) => ct_eq(digest(salt, password).as_bytes(), expected.as_bytes()),
        None => false,
    }
}
