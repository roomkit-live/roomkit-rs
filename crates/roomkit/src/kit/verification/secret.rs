//! A verification request's secret and its stored form (RFC §11.7.7), as
//! the Python implementation draws and hashes them, so that a request one
//! implementation opens the other completes on one store.

use std::fmt::Write as _;

use crate::error::RoomKitError;

/// The bytes of a secret carried in a link: 256 bits, RFC §11.7.7 asking for
/// at least 128 (Python's `_LINK_SECRET_BYTES`).
const LINK_SECRET_BYTES: usize = 32;

/// scrypt's work factor, `log2(N)`: a slow hash, not a fast one, since a
/// 6-digit code read from a leaked store would otherwise be found in under a
/// second. At `N = 2^15` one hash costs tens of milliseconds, so the million
/// codes take hours, against a code's minutes of life (Python's
/// `_SCRYPT_N = 2**15`).
pub(super) const SCRYPT_LOG_N: u8 = 15;
/// scrypt's block size (Python's `_SCRYPT_R`).
const SCRYPT_R: u32 = 8;
/// scrypt's parallelism (Python's `_SCRYPT_P`).
const SCRYPT_P: u32 = 1;
/// The bytes of a hash (Python's `dklen=32`).
const HASH_BYTES: usize = 32;
/// What a stored hash starts with.
const HASH_PREFIX: &str = "scrypt$";

/// A secret for a link: 32 random bytes, written as Python's
/// `secrets.token_urlsafe(32)` writes them, URL-safe base64 with no padding.
pub(super) fn link_secret() -> Result<String, RoomKitError> {
    let mut bytes = [0_u8; LINK_SECRET_BYTES];
    getrandom::fill(&mut bytes).map_err(random_failed)?;
    Ok(url_safe_base64(&bytes))
}

/// A code of `length` digits, drawn uniformly, as Python's
/// `secrets.randbelow(10**length)` padded with zeros.
pub(super) fn code(length: u32) -> Result<String, RoomKitError> {
    let bound = 10_u64
        .checked_pow(length)
        .ok_or_else(|| RoomKitError::Value(format!("a code of {length} digits")))?;
    // The largest multiple of `bound` a u64 holds: a draw at or past it is
    // drawn again, so that every code is as likely.
    let zone = u64::MAX - u64::MAX % bound;
    loop {
        let drawn = getrandom::u64().map_err(random_failed)?;
        if drawn < zone {
            let width = usize::try_from(length).unwrap_or(usize::MAX);
            return Ok(format!("{:0width$}", drawn % bound));
        }
    }
}

/// The stored form of `secret`: scrypt salted with its request's id,
/// `scrypt$<hex>`, as Python's `_secret_hash` writes it. Computed off the
/// runtime's workers, tens of milliseconds of CPU on purpose.
pub(super) async fn secret_hash(request_id: &str, secret: &str) -> Result<String, RoomKitError> {
    let (salt, secret) = (request_id.to_owned(), secret.to_owned());
    tokio::task::spawn_blocking(move || hash_with(SCRYPT_LOG_N, &salt, &secret))
        .await
        .map_err(|error| {
            RoomKitError::Internal(format!("the secret's hash did not end: {error}"))
        })?
}

/// The stored form of `secret` salted with `salt`, at the work factor
/// `log_n`.
pub(super) fn hash_with(log_n: u8, salt: &str, secret: &str) -> Result<String, RoomKitError> {
    let params = scrypt::Params::new(log_n, SCRYPT_R, SCRYPT_P)
        .map_err(|error| RoomKitError::Internal(format!("scrypt's parameters: {error}")))?;
    let mut digest = [0_u8; HASH_BYTES];
    scrypt::scrypt(secret.as_bytes(), salt.as_bytes(), &params, &mut digest)
        .map_err(|error| RoomKitError::Internal(format!("scrypt's output: {error}")))?;
    Ok(format!("{HASH_PREFIX}{}", hex(&digest)))
}

/// Whether two stored forms are the same, compared in constant time for
/// strings of one length, as Python's `hmac.compare_digest` compares them
/// (RFC §11.7.7).
pub(super) fn same_hash(stored: &str, computed: &str) -> bool {
    let (stored, computed) = (stored.as_bytes(), computed.as_bytes());
    if stored.len() != computed.len() {
        return false;
    }
    stored
        .iter()
        .zip(computed)
        .fold(0_u8, |differ, (a, b)| differ | (a ^ b))
        == 0
}

fn random_failed(error: getrandom::Error) -> RoomKitError {
    RoomKitError::Internal(format!("the system's random source failed: {error}"))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

/// `bytes` in URL-safe base64 with no padding (RFC 4648 §5), as Python's
/// `base64.urlsafe_b64encode(...).rstrip(b"=")` writes them.
fn url_safe_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut text = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        // Each group of bytes read gives one more character than bytes:
        // 1 → 2, 2 → 3, 3 → 4.
        let (group, characters) = match *chunk {
            [a] => ([a, 0, 0], 2),
            [a, b] => ([a, b, 0], 3),
            [a, b, c] => ([a, b, c], 4),
            _ => continue,
        };
        let [a, b, c] = group.map(u32::from);
        let bits = (a << 16) | (b << 8) | c;
        for position in 0..characters {
            let index = usize::try_from((bits >> (18 - 6 * position)) & 0x3f).unwrap_or_default();
            if let Some(&letter) = ALPHABET.get(index) {
                text.push(char::from(letter));
            }
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    // Python: tests/test_identity_verification.py::test_the_secret_hash_keeps_its_production_work_factor
    #[test]
    fn the_secret_hash_keeps_its_production_work_factor() {
        assert_eq!(1_u32 << SCRYPT_LOG_N, 2_u32.pow(15));
    }

    // Python: tests/test_identity_verification.py::test_a_secret_is_stored_through_a_slow_hash,
    // the hash of the scenario a-secret-is-checked-against-its-scrypt-hash:
    // hashlib.scrypt(b"s3cret", salt=b"req-1", n=2**15, r=8, p=1, dklen=32).
    #[tokio::test]
    async fn a_secret_is_stored_through_a_slow_hash() {
        let stored = secret_hash("req-1", "s3cret").await.unwrap();
        assert_eq!(
            stored,
            "scrypt$cfc5f8a1bfb9fe3c1bfed3e2a763e483ff22faede0cf65d56496df0197b58cc3"
        );
    }

    // Python: tests/test_identity_verification.py::test_a_code_request_sends_digits_and_lives_shorter,
    // the code's form: hashlib.scrypt(b"482913", salt=b"req-2", n=2**15, r=8, p=1, dklen=32).
    #[test]
    fn a_code_is_digits_of_the_policy_s_length_hashed_as_python_hashes_it() {
        for length in [6, 12] {
            let drawn = code(length).unwrap();
            assert_eq!(drawn.len(), usize::try_from(length).unwrap(), "{drawn}");
            assert!(drawn.bytes().all(|byte| byte.is_ascii_digit()), "{drawn}");
        }
        assert_eq!(
            hash_with(SCRYPT_LOG_N, "req-2", "482913").unwrap(),
            "scrypt$bd0827e54233f96be6ebadaed1f026b8fc64be554e34adc14dbfd64edd37bc2d"
        );
    }

    // Python: tests/test_identity_verification.py::test_the_secret_is_returned_once_and_never_stored,
    // the link secret's entropy: secrets.token_urlsafe(32).
    #[test]
    fn a_link_secret_carries_256_bits_url_safe() {
        let secret = link_secret().unwrap();
        assert_eq!(secret.len(), 43);
        assert!(
            secret
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
            "{secret}"
        );
        assert_ne!(secret, link_secret().unwrap());
    }

    // Python: base64.urlsafe_b64encode(b"\xfb\xff\x00").rstrip(b"="), and of
    // its first one and two bytes.
    #[test]
    fn url_safe_base64_writes_as_python_with_no_padding() {
        assert_eq!(url_safe_base64(&[0xfb, 0xff, 0x00]), "-_8A");
        assert_eq!(url_safe_base64(&[0xfb]), "-w");
        assert_eq!(url_safe_base64(&[0xfb, 0xff]), "-_8");
    }

    // Python: hmac.compare_digest, the comparison of complete_verification.
    #[test]
    fn hashes_of_another_length_or_content_differ() {
        assert!(same_hash("scrypt$ab", "scrypt$ab"));
        assert!(!same_hash("scrypt$ab", "scrypt$ac"));
        assert!(!same_hash("scrypt$ab", "scrypt$abc"));
    }
}
