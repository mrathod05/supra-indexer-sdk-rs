use crate::error::{IndexerError, Result};

/// Left-pad a hex address to 32 bytes (`0x` + 64 lowercase hex chars), so
/// addresses from different sources compare equal.
pub fn normalize_hex_to_fixed(input: &str) -> Result<String> {
    let s = input.trim();
    let hex = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s)
        .to_ascii_lowercase();

    if hex.len() > 64 {
        return Err(IndexerError::InvalidResponse(format!(
            "hex string `{s}` is longer than 32 bytes"
        )));
    }
    if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(IndexerError::InvalidResponse(format!(
            "`{s}` is not a hex string"
        )));
    }
    Ok(format!("0x{hex:0>64}"))
}

#[cfg(test)]
mod tests {
    use super::normalize_hex_to_fixed;

    #[test]
    fn pads_odd_length() {
        let out = normalize_hex_to_fixed(
            "0x5725e2fd119370a9da4b3afab923f9c35f454c810e175177f06a352de8a26d8",
        )
        .unwrap();
        assert_eq!(
            out,
            "0x05725e2fd119370a9da4b3afab923f9c35f454c810e175177f06a352de8a26d8"
        );
    }

    #[test]
    fn keeps_full_address() {
        let addr = "0x73d820fdc9febcbdb9824ce83d5939e6b4dd6cc251e8714a7da6eac64f2468bf";
        assert_eq!(normalize_hex_to_fixed(addr).unwrap(), addr);
    }

    #[test]
    fn handles_missing_prefix_case_and_whitespace() {
        assert_eq!(
            normalize_hex_to_fixed("  0XAbC ").unwrap(),
            "0x0000000000000000000000000000000000000000000000000000000000000abc"
        );
        assert_eq!(
            normalize_hex_to_fixed("1").unwrap(),
            "0x0000000000000000000000000000000000000000000000000000000000000001"
        );
    }

    #[test]
    fn rejects_invalid() {
        assert!(normalize_hex_to_fixed("0xzz").is_err());
        assert!(normalize_hex_to_fixed(&format!("0x{}", "1".repeat(65))).is_err());
    }
}
