use rust_decimal::Decimal;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("price overflow when scaling decimal {value} by 10^{scale}")]
pub struct PriceOverflow {
    pub value: Decimal,
    pub scale: u32,
}

/// Convert a Decimal price/size to a scaled i64 representation.
///
/// Used by both Parquet sink (Arrow Int64 columns) and Redis sink
/// (companion `*_i64` fields in JSON) to guarantee identical rounding.
pub fn to_scaled_i64(value: Decimal, scale: u32) -> Result<i64, PriceOverflow> {
    let multiplier = Decimal::from(10u64.pow(scale));
    let scaled = value
        .checked_mul(multiplier)
        .ok_or(PriceOverflow { value, scale })?;
    scaled
        .to_string()
        .parse::<i64>()
        .map_err(|_| PriceOverflow { value, scale })
}

/// Convert a scaled i64 back to a Decimal.
pub fn from_scaled_i64(raw: i64, scale: u32) -> Decimal {
    let divisor = Decimal::from(10u64.pow(scale));
    Decimal::from(raw) / divisor
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn round_trip() {
        let price = dec!(97234.56);
        let scale = 8;
        let scaled = to_scaled_i64(price, scale).unwrap();
        assert_eq!(scaled, 9_723_456_000_000i64);
        let back = from_scaled_i64(scaled, scale);
        assert_eq!(back, price);
    }

    #[test]
    fn zero() {
        assert_eq!(to_scaled_i64(Decimal::ZERO, 8).unwrap(), 0);
    }
}
