//! Built-in strategy descriptor catalog.
//!
//! The authoritative, schema-driven descriptions of the built-in strategies
//! that the terminal exposes to users. The UI renders configuration forms
//! from [`StrategyDescriptor`]`::params` without any per-strategy hard-coding;
//! extending the catalog with a new descriptor is the only change needed to
//! surface a new built-in strategy.

#![forbid(unsafe_code)]

use longtrader_contract::proto::longtrader::terminal::v1::{ParamSlot, StrategyDescriptor};

/// Built-in strategy ids.
pub mod ids {
    /// Grid market-making: layered resting orders across a price grid.
    pub const GRID_MAKER: &str = "grid_maker";
    /// Cross-venue convergence arbitrage: long spot / short perp (or vice
    /// versa) opened on a basis threshold and closed on a tighter one.
    pub const CMDNC_ARB: &str = "cmdnc_arb";
}

/// Returns every built-in strategy descriptor.
#[must_use]
pub fn builtin_descriptors() -> Vec<StrategyDescriptor> {
    vec![grid_maker(), cmdnc_arb()]
}

/// Looks up a built-in descriptor by id.
#[must_use]
pub fn descriptor(id: &str) -> Option<StrategyDescriptor> {
    builtin_descriptors().into_iter().find(|d| d.id == id)
}

fn slot(
    label: &str,
    dimension: &str,
    min: Option<f64>,
    max: Option<f64>,
    step: Option<f64>,
    default: Option<f64>,
) -> ParamSlot {
    ParamSlot {
        label: label.into(),
        dimension: dimension.into(),
        min,
        max,
        step,
        default,
        ..Default::default()
    }
}

/// `grid_maker`: resting limit orders on a symmetric grid around the mark.
fn grid_maker() -> StrategyDescriptor {
    StrategyDescriptor {
        id: ids::GRID_MAKER.into(),
        name: "Grid Maker".into(),
        note: "Layer resting limit orders from lower to upper bound: each level rests quantity per level with an offset from mark; buy low / sell high, spread is round-trip edge.".into(),
        builtin: true,
        kind: "maker".into(),
        params: vec![
            slot("lower_bound", "usd", Some(0.0), None, Some(1.0), Some(90_000.0)),
            slot("upper_bound", "usd", Some(0.0), None, Some(1.0), Some(110_000.0)),
            slot("levels", "decimal", Some(1.0), None, Some(1.0), Some(20.0)),
            slot("qty_per_level", "decimal", Some(0.0), None, Some(0.0001), Some(0.01)),
            slot("order_offset", "percent", Some(0.0), None, Some(0.0001), Some(0.001)),
            slot("max_notional", "usd", Some(0.0), None, Some(50.0), Some(1_000.0)),
        ],
        ..Default::default()
    }
}

/// `cmdnc_arb`: cross-venue basis / funding-rate convergence arbitrage.
fn cmdnc_arb() -> StrategyDescriptor {
    StrategyDescriptor {
        id: ids::CMDNC_ARB.into(),
        name: "Cross-Venue Convergence Arbitrage".into(),
        note: "Paired cross-venue legs on one symbol: open to per-round notional when basis reaches open threshold, flatten at close threshold, hold between.".into(),
        builtin: true,
        kind: "arb".into(),
        params: vec![
            slot("open_basis", "percent", Some(0.0), None, Some(0.0001), Some(0.005)),
            slot("close_basis", "percent", Some(0.0), None, Some(0.0001), Some(0.002)),
            slot("notional_per_round", "usd", Some(0.0), None, Some(10.0), Some(100.0)),
            slot("max_notional", "usd", Some(0.0), None, Some(50.0), Some(1_000.0)),
        ],
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_contains_only_the_two_builtins() {
        let descriptors = builtin_descriptors();
        let ids: Vec<&str> = descriptors.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(ids, [ids::GRID_MAKER, ids::CMDNC_ARB]);
    }

    #[test]
    fn every_descriptor_is_builtin_and_has_slots() {
        for d in builtin_descriptors() {
            assert!(d.builtin, "{} must be builtin", d.id);
            assert!(!d.params.is_empty(), "{} must declare param slots", d.id);
            for (i, p) in d.params.iter().enumerate() {
                assert!(!p.label.is_empty(), "{}.params[{i}].label empty", d.id);
                assert!(!p.dimension.is_empty(), "{}.params[{i}].dimension empty", d.id);
            }
        }
    }

    #[test]
    fn lookup_by_id_round_trips() {
        assert_eq!(
            descriptor(ids::GRID_MAKER).expect("builtin descriptor GRID_MAKER must exist").id,
            ids::GRID_MAKER
        );
        assert_eq!(
            descriptor(ids::CMDNC_ARB).expect("builtin descriptor CMDNC_ARB must exist").id,
            ids::CMDNC_ARB
        );
        assert!(descriptor("nope").is_none());
    }
}
