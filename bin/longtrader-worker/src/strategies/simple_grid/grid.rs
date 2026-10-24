//! Pure grid arithmetic for [`super::SimpleGrid`].
//!
//! Everything here is a function of the configuration and a price — no ports,
//! no clock, no I/O — so the ladder geometry (step, side rule, level count) is
//! unit-testable without a venue and cannot acquire a dependency on one.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::{GridLevel, SimpleGridConfig};
use crate::proto::trading;

/// Distance between adjacent rungs.
///
/// `0` for a zero-level grid and negative for an inverted span: both are
/// guarded by [`calculate_levels`], which refuses to quote either.
#[inline]
#[must_use]
pub(super) fn grid_step(config: &SimpleGridConfig) -> Decimal {
    if config.num_levels > 0 {
        (config.upper_price - config.lower_price) / Decimal::from(config.num_levels)
    } else {
        dec!(0)
    }
}

/// Which side a rung quotes: below the market buys, at or above it sells.
#[inline]
#[must_use]
pub(super) fn side_for(price: Decimal, current: Decimal) -> trading::OrderSide {
    if price < current { trading::OrderSide::Buy } else { trading::OrderSide::Sell }
}

/// The ladder: every rung from `lower_price` to `upper_price` inclusive,
/// spaced by `grid_step`, each carrying a fresh client order id.
///
/// An empty ladder for a non-positive step or a span that cannot hold a rung —
/// a degenerate configuration is quoted as nothing rather than as one giant
/// order at the bound.
#[must_use]
pub(super) fn calculate_levels(
    config: &SimpleGridConfig,
    grid_step: Decimal,
    current_price: Decimal,
) -> Vec<GridLevel> {
    let mut levels = Vec::new();
    if grid_step <= dec!(0) {
        return levels;
    }
    let max_levels = usize::try_from(config.num_levels).unwrap_or(usize::MAX);
    let mut price = config.lower_price;
    while price <= config.upper_price && levels.len() <= max_levels {
        levels.push(GridLevel {
            price,
            side: side_for(price, current_price),
            client_order_id: format!("grid-{}", ulid::Ulid::generate()),
            order_id: String::new(),
        });
        price += grid_step;
    }
    levels
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;
    use crate::strategies::simple_grid::SimpleGridConfig;

    fn config(lower: Decimal, upper: Decimal, num_levels: u32) -> SimpleGridConfig {
        SimpleGridConfig {
            exchange_id: crate::ports::exchange_id("mock", ""),
            symbol: "BTC/USDT".to_string(),
            lower_price: lower,
            upper_price: upper,
            num_levels,
            qty_per_level: dec!(0.1),
            profit_spread_pct: None,
            state_file: None,
        }
    }

    #[test]
    fn a_zero_level_grid_has_a_zero_step_and_no_levels() {
        let cfg = config(dec!(90), dec!(110), 0);
        assert_eq!(grid_step(&cfg), Decimal::ZERO);
        assert!(calculate_levels(&cfg, grid_step(&cfg), dec!(100)).is_empty());
    }

    /// An inverted span yields a negative step, which the `grid_step <= 0`
    /// guard turns into an empty ladder rather than a panic or a hang.
    #[test]
    fn an_inverted_span_produces_no_levels() {
        let cfg = config(dec!(110), dec!(90), 4);
        assert!(grid_step(&cfg) < Decimal::ZERO);
        assert!(calculate_levels(&cfg, grid_step(&cfg), dec!(100)).is_empty());
    }

    #[test]
    fn a_collapsed_span_produces_no_levels() {
        let cfg = config(dec!(100), dec!(100), 4);
        assert_eq!(grid_step(&cfg), Decimal::ZERO);
        assert!(calculate_levels(&cfg, grid_step(&cfg), dec!(100)).is_empty());
    }

    #[test]
    fn the_ladder_is_inclusive_of_both_bounds_and_evenly_spaced() {
        let cfg = config(dec!(90), dec!(110), 4);
        let step = grid_step(&cfg);
        assert_eq!(step, dec!(5));
        let levels = calculate_levels(&cfg, step, dec!(100));
        assert_eq!(levels.len(), 5, "90, 95, 100, 105, 110");
        assert!(levels.windows(2).all(|w| w[1].price - w[0].price == step));
        assert_eq!(levels.first().expect("first").price, dec!(90));
        assert_eq!(levels.last().expect("last").price, dec!(110));
    }

    #[test]
    fn ladder_sides_follow_the_current_price() {
        let cfg = config(dec!(90), dec!(110), 4);
        let levels = calculate_levels(&cfg, grid_step(&cfg), dec!(100));
        assert!(
            levels
                .iter()
                .filter(|l| l.price < dec!(100))
                .all(|l| l.side == trading::OrderSide::Buy)
        );
        assert!(
            levels
                .iter()
                .filter(|l| l.price >= dec!(100))
                .all(|l| l.side == trading::OrderSide::Sell)
        );
    }

    #[test]
    fn the_ladder_never_exceeds_the_level_count_plus_one() {
        let cfg = config(dec!(90), dec!(110), 3);
        let levels = calculate_levels(&cfg, grid_step(&cfg), dec!(100));
        assert!(levels.len() <= 4, "3 levels means at most 4 rungs, got {}", levels.len());
    }

    #[test]
    fn the_side_rule_holds_for_degenerate_prices() {
        assert_eq!(side_for(dec!(99), dec!(100)), trading::OrderSide::Buy);
        assert_eq!(side_for(dec!(100), dec!(100)), trading::OrderSide::Sell);
        assert_eq!(side_for(dec!(101), dec!(100)), trading::OrderSide::Sell);
        assert_eq!(side_for(Decimal::ZERO, dec!(100)), trading::OrderSide::Buy);
        assert_eq!(side_for(dec!(0.00000001), Decimal::ZERO), trading::OrderSide::Sell);
        assert_eq!(side_for(Decimal::ZERO, Decimal::ZERO), trading::OrderSide::Sell);
        assert_eq!(side_for(dec!(-2), dec!(-1)), trading::OrderSide::Buy);
        assert_eq!(side_for(dec!(-1), dec!(-2)), trading::OrderSide::Sell);
        assert_eq!(side_for(dec!(-1), dec!(-1)), trading::OrderSide::Sell);
    }
}
