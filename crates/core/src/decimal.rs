//! Fixed-point decimal arithmetic.
//!
//! The hot path represents prices and quantities as signed integers in
//! units of `10^-scale`. The scale itself lives on the [`InstrumentSpec`]
//! so `Price` and `Qty` stay a single `i64` — no allocations, no
//! floating-point drift, exact equality.
//!
//! [`InstrumentSpec`]: crate::venue::InstrumentSpec

use std::fmt;

/// Maximum fractional scale accepted by this module. 18 is enough to
/// represent ETH wei without overflowing `i64` for any realistic notional.
pub const MAX_SCALE: u8 = 18;

/// Precomputed powers of ten up to `10^MAX_SCALE`.
const POW10: [i64; 19] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
];

/// Fixed-point price expressed at an instrument-specific scale.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(pub i64);

/// Fixed-point quantity expressed at an instrument-specific scale.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Qty(pub i64);

/// Errors the decimal helpers can produce.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecimalError {
    /// Scale exceeded [`MAX_SCALE`].
    ScaleTooLarge,
    /// Literal could not be parsed.
    InvalidNumber,
    /// Intermediate value overflowed `i64`.
    Overflow,
}

impl fmt::Display for DecimalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecimalError::ScaleTooLarge => write!(f, "scale exceeds MAX_SCALE"),
            DecimalError::InvalidNumber => write!(f, "invalid decimal literal"),
            DecimalError::Overflow => write!(f, "decimal overflow"),
        }
    }
}

impl std::error::Error for DecimalError {}

/// Parse a base-10 numeric string into an `i64` mantissa at the given scale.
///
/// Accepts an optional leading sign, an integer part, and an optional
/// fractional part. Rejects scientific notation and stray non-digits.
/// Excess fractional digits are truncated toward zero, matching how most
/// exchanges report ticks.
pub fn parse_decimal(s: &str, scale: u8) -> Result<i64, DecimalError> {
    if scale > MAX_SCALE {
        return Err(DecimalError::ScaleTooLarge);
    }
    if s.is_empty() {
        return Err(DecimalError::InvalidNumber);
    }

    let bytes = s.as_bytes();
    let mut idx = 0;
    let mut neg = false;
    if bytes[0] == b'-' {
        neg = true;
        idx = 1;
        if idx == bytes.len() {
            return Err(DecimalError::InvalidNumber);
        }
    }

    let mut mantissa: i64 = 0;
    let mut seen_digit = false;

    // Integer part.
    while idx < bytes.len() && bytes[idx] != b'.' {
        let c = bytes[idx];
        if !c.is_ascii_digit() {
            return Err(DecimalError::InvalidNumber);
        }
        mantissa = mantissa
            .checked_mul(10)
            .and_then(|m| m.checked_add((c - b'0') as i64))
            .ok_or(DecimalError::Overflow)?;
        seen_digit = true;
        idx += 1;
    }

    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        let need = scale as usize;
        let mut consumed = 0;
        while idx < bytes.len() && consumed < need {
            let c = bytes[idx];
            if !c.is_ascii_digit() {
                return Err(DecimalError::InvalidNumber);
            }
            mantissa = mantissa
                .checked_mul(10)
                .and_then(|m| m.checked_add((c - b'0') as i64))
                .ok_or(DecimalError::Overflow)?;
            consumed += 1;
            seen_digit = true;
            idx += 1;
        }
        // Pad remaining fraction digits with zeros.
        while consumed < need {
            mantissa = mantissa.checked_mul(10).ok_or(DecimalError::Overflow)?;
            consumed += 1;
        }
        // Validate any truncated tail is still digits.
        while idx < bytes.len() {
            if !bytes[idx].is_ascii_digit() {
                return Err(DecimalError::InvalidNumber);
            }
            idx += 1;
        }
    } else {
        // Pure integer: shift up by the scale.
        let shift = POW10[scale as usize];
        if mantissa != 0 {
            mantissa = mantissa.checked_mul(shift).ok_or(DecimalError::Overflow)?;
        }
    }

    if !seen_digit {
        return Err(DecimalError::InvalidNumber);
    }

    Ok(if neg { -mantissa } else { mantissa })
}

/// Render a fixed-point mantissa back to base-10 with trailing zeros
/// trimmed (but the integer part is preserved).
pub fn format_decimal(m: i64, scale: u8) -> String {
    if scale == 0 || scale > MAX_SCALE {
        return m.to_string();
    }
    let neg = m < 0;
    // i64::MIN edge case: abs() would overflow. Handle with i128.
    let abs = (m as i128).unsigned_abs();
    let div = POW10[scale as usize] as u128;
    let int_part = abs / div;
    let frac_part = abs % div;

    let mut out = String::new();
    if neg {
        out.push('-');
    }
    out.push_str(&int_part.to_string());

    // Render fractional part zero-padded to `scale`, then trim trailing zeros.
    let mut frac = frac_part.to_string();
    let needed = scale as usize;
    if frac.len() < needed {
        let pad = needed - frac.len();
        let mut padded = String::with_capacity(needed);
        for _ in 0..pad {
            padded.push('0');
        }
        padded.push_str(&frac);
        frac = padded;
    }
    let trimmed = frac.trim_end_matches('0');
    if !trimmed.is_empty() {
        out.push('.');
        out.push_str(trimmed);
    }
    out
}

/// Convert a float to a fixed-point mantissa. Rounds half-away-from-zero.
/// Use only at system boundaries; internal state should stay integer.
pub fn float_to_decimal(f: f64, scale: u8) -> Result<i64, DecimalError> {
    if scale > MAX_SCALE {
        return Err(DecimalError::ScaleTooLarge);
    }
    if !f.is_finite() {
        return Err(DecimalError::InvalidNumber);
    }
    let scaled = f * POW10[scale as usize] as f64;
    if scaled > i64::MAX as f64 || scaled < i64::MIN as f64 {
        return Err(DecimalError::Overflow);
    }
    Ok(if scaled >= 0.0 {
        (scaled + 0.5) as i64
    } else {
        (scaled - 0.5) as i64
    })
}

/// Convert a fixed-point mantissa to a float (lossy).
pub fn decimal_to_float(m: i64, scale: u8) -> f64 {
    if scale > MAX_SCALE {
        return m as f64;
    }
    m as f64 / POW10[scale as usize] as f64
}

// ---------- ergonomic helpers on Price / Qty ----------

impl Price {
    pub fn from_str(s: &str, scale: u8) -> Result<Self, DecimalError> {
        parse_decimal(s, scale).map(Price)
    }
    pub fn from_f64(f: f64, scale: u8) -> Result<Self, DecimalError> {
        float_to_decimal(f, scale).map(Price)
    }
    pub fn to_string(self, scale: u8) -> String {
        format_decimal(self.0, scale)
    }
    pub fn to_f64(self, scale: u8) -> f64 {
        decimal_to_float(self.0, scale)
    }
}

impl Qty {
    pub fn from_str(s: &str, scale: u8) -> Result<Self, DecimalError> {
        parse_decimal(s, scale).map(Qty)
    }
    pub fn from_f64(f: f64, scale: u8) -> Result<Self, DecimalError> {
        float_to_decimal(f, scale).map(Qty)
    }
    pub fn to_string(self, scale: u8) -> String {
        format_decimal(self.0, scale)
    }
    pub fn to_f64(self, scale: u8) -> f64 {
        decimal_to_float(self.0, scale)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_happy_paths() {
        let cases: &[(&str, u8, i64)] = &[
            ("0", 8, 0),
            ("1", 0, 1),
            ("1", 8, 100_000_000),
            ("1.5", 2, 150),
            ("123.456", 4, 1_234_560),
            ("-42.0", 2, -4_200),
            ("0.00000001", 8, 1),
            ("99999999.99999999", 8, 9_999_999_999_999_999),
            // Truncation toward zero.
            ("1.123456789", 4, 11_234),
            ("-1.999999", 2, -199),
        ];
        for &(s, scale, want) in cases {
            let got = parse_decimal(s, scale).unwrap_or_else(|e| panic!("parse {s}: {e}"));
            assert_eq!(got, want, "parse({s:?}, {scale})");
        }
    }

    #[test]
    fn parse_rejects_garbage() {
        for (s, scale) in &[
            ("", 8),
            ("-", 8),
            ("abc", 8),
            ("1.2.3", 8),
            ("1e5", 8),
            ("1.2x", 4),
            ("1", 19),
        ] {
            assert!(
                parse_decimal(s, *scale).is_err(),
                "expected error for ({s:?}, {scale})"
            );
        }
    }

    #[test]
    fn format_happy_paths() {
        let cases: &[(i64, u8, &str)] = &[
            (0, 8, "0"),
            (100_000_000, 8, "1"),
            (150, 2, "1.5"),
            (1_234_560, 4, "123.456"),
            (-4_200, 2, "-42"),
            (1, 8, "0.00000001"),
            (9_999_999_999_999_999, 8, "99999999.99999999"),
        ];
        for &(m, scale, want) in cases {
            assert_eq!(format_decimal(m, scale), want, "format({m}, {scale})");
        }
    }

    #[test]
    fn parse_format_roundtrip() {
        for &input in &[
            "0",
            "1",
            "1.5",
            "123.456",
            "-42",
            "0.00000001",
            "99999999.99999999",
        ] {
            let m = parse_decimal(input, 8).unwrap();
            let rendered = format_decimal(m, 8);
            let m2 = parse_decimal(&rendered, 8).unwrap();
            assert_eq!(m, m2, "roundtrip drift: {input} -> {rendered}");
        }
    }

    #[test]
    fn float_roundtrip_within_epsilon() {
        for &(f, scale) in &[
            (0.0, 8u8),
            (1.5, 2),
            (123.456, 4),
            (-42.25, 2),
            (0.00000001, 8),
        ] {
            let m = float_to_decimal(f, scale).unwrap();
            let back = decimal_to_float(m, scale);
            assert!((back - f).abs() < 1e-9, "{f} -> {back}");
        }
    }

    #[test]
    fn float_rejects_nan_inf() {
        assert!(float_to_decimal(f64::NAN, 4).is_err());
        assert!(float_to_decimal(f64::INFINITY, 4).is_err());
        assert!(float_to_decimal(f64::NEG_INFINITY, 4).is_err());
    }

    #[test]
    fn price_qty_helpers() {
        let p = Price::from_str("65000.50", 2).unwrap();
        assert_eq!(p.0, 6_500_050);
        assert_eq!(p.to_string(2), "65000.5");
        assert!((p.to_f64(2) - 65_000.50).abs() < 1e-9);

        let q = Qty::from_str("0.0001", 8).unwrap();
        assert_eq!(q.0, 10_000);
    }

    #[test]
    fn overflow_detected() {
        // 9.22e18 fits in i64, this does not.
        let huge = "9223372036854775808";
        assert_eq!(parse_decimal(huge, 0), Err(DecimalError::Overflow));
    }
}
