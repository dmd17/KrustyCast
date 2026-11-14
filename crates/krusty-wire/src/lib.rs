//! KrustyCast native wire formats (ARCHITECTURE §8, ADR-8).
//!
//! - [`ingress`]: client → cluster submissions.
//! - [`feedback`]: cluster → client outcomes.
//! - [`stream`]: the native stream (cluster → consumers) and its recovery
//!   requests (consumer → cluster).
//!
//! Every format is a fixed little-endian layout that starts with the same
//! 4 bytes: `magic: u16 | version: u8 | type: u8`. Fields sit at naturally
//! aligned offsets. Decoding is zero-copy (views borrow the datagram), never
//! allocates and never panics: every length and range is checked once, up
//! front, so the accessors that follow are infallible. Encoding writes into
//! a caller-provided buffer.
//!
//! The crate has no `unsafe`, no dependencies and no allocation, so Kani and
//! Aeneas can check it.
#![no_std]
#![forbid(unsafe_code)]

pub mod feedback;
pub mod ingress;
pub mod stream;

/// First two bytes of every KrustyCast datagram ("KC").
pub const MAGIC: u16 = 0x434B;
/// Wire format version understood by this crate.
pub const VERSION: u8 = 1;
/// Size of the common prefix: magic, version, type.
pub const PREFIX_LEN: usize = 4;

/// Message type codes, unique across all formats so a stray datagram is
/// never misread as another format.
pub mod msg_type {
    pub const SUBMIT: u8 = 0x01;

    pub const ACCEPTED: u8 = 0x10;
    pub const REJECTED: u8 = 0x11;
    pub const NAK: u8 = 0x12;
    pub const BUSY: u8 = 0x13;
    pub const NOT_LEADER: u8 = 0x14;
    pub const SESSION_EXPIRED: u8 = 0x15;

    pub const DATA: u8 = 0x20;
    pub const HEARTBEAT: u8 = 0x21;
    pub const END_OF_SESSION: u8 = 0x22;
    pub const UNAVAILABLE: u8 = 0x23;
    pub const RECOVER: u8 = 0x30;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    /// Shorter than the fixed header, or than the lengths it declares.
    Truncated,
    /// Bytes left over after the last declared field.
    TrailingBytes,
    BadMagic,
    UnsupportedVersion(u8),
    UnknownType(u8),
    /// A field holds a value the format does not allow.
    BadField(&'static str),
    /// A payload is larger than the format can carry.
    TooLarge,
    /// The output buffer is too small.
    BufferTooSmall,
}

/// The message type of a datagram after checking magic and version.
pub fn peek_type(b: &[u8]) -> Result<u8, WireError> {
    if b.len() < PREFIX_LEN {
        return Err(WireError::Truncated);
    }
    if get_u16(b, 0) != MAGIC {
        return Err(WireError::BadMagic);
    }
    if b[2] != VERSION {
        return Err(WireError::UnsupportedVersion(b[2]));
    }
    Ok(b[3])
}

fn check_prefix(b: &[u8], expected_type: u8) -> Result<(), WireError> {
    let t = peek_type(b)?;
    if t != expected_type {
        return Err(WireError::UnknownType(t));
    }
    Ok(())
}

fn put_prefix(out: &mut [u8], msg_type: u8) {
    put_u16(out, 0, MAGIC);
    out[2] = VERSION;
    out[3] = msg_type;
}

// Little-endian accessors. Callers check bounds first.

fn get_u16(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}

fn get_u32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

fn get_u64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes([
        b[at],
        b[at + 1],
        b[at + 2],
        b[at + 3],
        b[at + 4],
        b[at + 5],
        b[at + 6],
        b[at + 7],
    ])
}

fn put_u16(b: &mut [u8], at: usize, v: u16) {
    b[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

fn put_u32(b: &mut [u8], at: usize, v: u32) {
    b[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

fn put_u64(b: &mut [u8], at: usize, v: u64) {
    b[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

/// Rounds up to a multiple of 8.
const fn pad8(n: usize) -> usize {
    (n + 7) & !7
}
