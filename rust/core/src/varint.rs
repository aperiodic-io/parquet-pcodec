//! Unsigned LEB128, at most 10 bytes, below 2^64 (spec section 4).

use crate::Error;

pub(crate) fn put(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Reads one varint from the front of `src`, advancing it.
pub(crate) fn get(src: &mut &[u8]) -> Result<u64, Error> {
    let mut v: u64 = 0;
    for i in 0..10 {
        let (&b, rest) = src.split_first().ok_or(Error::Truncated)?;
        *src = rest;
        let part = u64::from(b & 0x7F);
        // the tenth byte may only carry the top bit of a u64
        if i == 9 && part > 1 {
            return Err(Error::Truncated);
        }
        v |= part << (7 * i);
        if b < 0x80 {
            return Ok(v);
        }
    }
    Err(Error::Truncated)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: u64) -> Vec<u8> {
        let mut o = vec![];
        put(&mut o, v);
        o
    }

    #[test]
    fn known_encodings() {
        assert_eq!(enc(0), [0]);
        assert_eq!(enc(1), [1]);
        assert_eq!(enc(127), [0x7F]);
        assert_eq!(enc(128), [0x80, 0x01]);
        assert_eq!(enc(300), [0xAC, 0x02]);
        assert_eq!(enc(u64::MAX), [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
    }

    #[test]
    fn round_trip_and_advance() {
        for v in [0, 1, 127, 128, 255, 16_383, 16_384, 1 << 35, u64::MAX - 1, u64::MAX] {
            let mut b = enc(v);
            b.push(0xEE);
            let mut s = &b[..];
            assert_eq!(get(&mut s).unwrap(), v);
            assert_eq!(s, [0xEE]);
        }
    }

    #[test]
    fn rejects_overlong_and_truncated() {
        let mut s: &[u8] = &[];
        assert_eq!(get(&mut s), Err(Error::Truncated));
        let mut s: &[u8] = &[0x80];
        assert_eq!(get(&mut s), Err(Error::Truncated));
        // 2^64 does not fit
        let mut s: &[u8] = &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02];
        assert_eq!(get(&mut s), Err(Error::Truncated));
        // eleven bytes
        let mut s: &[u8] = &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x81, 0x00];
        assert_eq!(get(&mut s), Err(Error::Truncated));
    }
}
