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

/// Every strategy the worker registry can build (`bin/longtrader-worker/src/
/// strategies/registry.rs`). Kept here so the terminal UI can list every
/// executable strategy even before it gains a fully specified numeric schema.
/// A stub has no numeric `params` (string-only strategies such as `symbol`
/// cannot be expressed as `ParamSlot` doubles); the UI renders a TOML editor
/// for those until a detailed schema lands. Detailed entries above (`grid_maker`,
/// `cmdnc_arb`) keep their numeric slots.
pub const WORKER_STRATEGY_IDS: [&str; 55] = [
    "adx_trend",
    "amm_arb",
    "apt_mm",
    "autoborrow",
    "avellaneda_mm_v2",
    "balance_align",
    "basis_mm",
    "bb_keltner_squeeze",
    "boll_grid",
    "bollinger_v2",
    "convert",
    "cross_depth_maker",
    "cross_exchange_mm",
    "cross_fixed_maker",
    "cross_maker",
    "cvd_divergence",
    "darvas_box",
    "dca",
    "dca_executor",
    "dca_scheduler",
    "deposit_transfer",
    "dman_v3",
    "dual_thrust",
    "ema_cross",
    "fixed_maker",
    "glft_mm",
    "grid_strike",
    "hedge",
    "hedge_grid",
    "ichimoku",
    "irr",
    "liquidity_mining",
    "macd_bb",
    "market_cap",
    "mean_reversion",
    "momentum_factor",
    "nav_recorder",
    "obi_mm",
    "perpetual_mm",
    "pmm_dynamic",
    "premium_monitor",
    "pure_market_making",
    "queue_mm",
    "random_entry",
    "rebalance",
    "sentinel",
    "simple_grid",
    "simplified_glft_mm",
    "skewed_grid",
    "spot_perp_arb",
    "stat_arb",
    "supertrend",
    "twap_executor",
    "vwap_executor",
    "xfunding_lite",
];

/// Kinds for worker strategies that are not market makers: the terminal UI
/// groups and filters by `kind`, so a mislabeled stub would hide a strategy
/// under the wrong filter. Anything absent here is a maker.
const ARB_STRATEGY_IDS: [&str; 3] = ["amm_arb", "spot_perp_arb", "stat_arb"];
const HEDGE_STRATEGY_IDS: [&str; 2] = ["hedge", "hedge_grid"];

/// Minimal descriptor for a worker strategy without a detailed numeric schema.
fn worker_stub(id: &'static str) -> StrategyDescriptor {
    // Human name: `snake_case` -> `Title Words`.
    let name = id
        .split('_')
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                Some(f) => f.to_ascii_uppercase().to_string() + c.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    let kind = if ARB_STRATEGY_IDS.contains(&id) {
        "arb"
    } else if HEDGE_STRATEGY_IDS.contains(&id) {
        "hedge"
    } else {
        "maker"
    };
    StrategyDescriptor {
        id: id.into(),
        name,
        note: "Worker strategy: configure via TOML until a detailed numeric schema lands.".into(),
        builtin: true,
        kind: kind.into(),
        // No numeric slots: every `ParamSlot` is a double, which cannot express
        // the string-valued params (symbols, venues) these strategies take.
        // The `..Default::default()` below supplies the empty params list.
        ..Default::default()
    }
}

/// Returns every built-in strategy descriptor: the two fully specified terminal
/// templates plus one stub per worker registry entry so no executable strategy
/// is invisible to the UI.
///
/// Uniqueness across the two groups is enforced by
/// `descriptor_ids_are_unique_non_empty_and_snake_case`: if a worker ever
/// adopts a terminal-only id, that test fails and forces an explicit decision
/// instead of silently emitting two entries for one id.
#[must_use]
pub fn builtin_descriptors() -> Vec<StrategyDescriptor> {
    let mut out = vec![grid_maker(), cmdnc_arb()];
    for id in WORKER_STRATEGY_IDS {
        out.push(worker_stub(id));
    }
    out
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
    use std::collections::BTreeSet;

    use proptest::prelude::*;

    use super::*;

    // The two vocabularies below are transcribed verbatim from
    // proto/longtrader/terminal/v1/strategy.proto:
    //
    //   ParamSlot.dimension:            "Dimension: percent / usd / decimal / duration /
    //                                    timestamp."
    //   StrategyDescriptor.kind:        "Strategy type: hedge (two-leg combination
    //                                    hedging) / maker (market making) / arb
    //                                    (arbitrage)."
    //
    // They are the only authority for what the UI config forms accept; a slot or
    // descriptor outside these sets would render an unusable control.
    const DIMENSIONS: [&str; 5] = ["percent", "usd", "decimal", "duration", "timestamp"];
    const KINDS: [&str; 3] = ["hedge", "maker", "arb"];

    /// `min <= default`; vacuously true when either endpoint is unset.
    fn min_le_default(p: &ParamSlot) -> bool {
        match (p.min, p.default) {
            (Some(min), Some(default)) => min <= default,
            _ => true,
        }
    }

    /// `default <= max`; vacuously true when either endpoint is unset.
    fn default_le_max(p: &ParamSlot) -> bool {
        match (p.default, p.max) {
            (Some(default), Some(max)) => default <= max,
            _ => true,
        }
    }

    /// A fully bounded slot must expose a strictly increasing range; vacuously
    /// true when either endpoint is unset.
    fn strict_min_lt_max(p: &ParamSlot) -> bool {
        match (p.min, p.max) {
            (Some(min), Some(max)) => min < max,
            _ => true,
        }
    }

    /// `step`, when present, must be strictly positive and finite.
    fn step_is_positive_finite(p: &ParamSlot) -> bool {
        match p.step {
            Some(step) => step.is_finite() && step > 0.0,
            None => true,
        }
    }

    /// Every present numeric field must be finite: no `NaN`, no `+/-inf`. All four
    /// fields are `optional double` on the wire, so `NaN` and the infinities are
    /// perfectly representable and would silently poison the UI sliders.
    fn numerics_are_finite(p: &ParamSlot) -> bool {
        [p.min, p.max, p.step, p.default].into_iter().flatten().all(f64::is_finite)
    }

    fn dimension_is_documented(dimension: &str) -> bool {
        DIMENSIONS.contains(&dimension)
    }

    fn kind_is_documented(kind: &str) -> bool {
        KINDS.contains(&kind)
    }

    fn id_is_snake_case(id: &str) -> bool {
        let mut chars = id.chars();
        let Some(first) = chars.next() else {
            return false;
        };
        if !first.is_ascii_lowercase() {
            return false;
        }
        id.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    }

    /// Runs `f` over every `(descriptor, slot)` pair in the catalog.
    fn for_each_slot(f: impl Fn(&StrategyDescriptor, &ParamSlot)) {
        for d in builtin_descriptors() {
            for p in &d.params {
                f(&d, p);
            }
        }
    }

    /// Every slot in the catalog, flattened, in declaration order.
    fn all_slots() -> Vec<ParamSlot> {
        builtin_descriptors().into_iter().flat_map(|d| d.params).collect()
    }

    fn assert_finite_field(d: &StrategyDescriptor, p: &ParamSlot, field: &str, v: Option<f64>) {
        let Some(v) = v else { return };
        assert!(!v.is_nan(), "{}.{}: {field} is NaN", d.id, p.label);
        assert!(!v.is_infinite(), "{}.{}: {field} is infinite", d.id, p.label);
        assert!(v.is_finite(), "{}.{}: {field} is not finite", d.id, p.label);
    }

    /// Arbitrary *finite* `f64`: every float class `proptest::num::f64::ANY` offers
    /// except infinity and NaN. Both signed zeros are included on purpose, because
    /// `-0.0` is representable on the wire and must satisfy the same invariants.
    fn arb_finite_f64() -> impl Strategy<Value = f64> {
        prop::num::f64::POSITIVE |
            prop::num::f64::NEGATIVE |
            prop::num::f64::NORMAL |
            prop::num::f64::SUBNORMAL |
            prop::num::f64::ZERO
    }

    #[test]
    fn catalog_contains_the_two_detailed_builtins_plus_worker_stubs() {
        let descriptors = builtin_descriptors();
        let ids: Vec<&str> = descriptors.iter().map(|d| d.id.as_str()).collect();
        // The two fully specified templates lead; every worker id follows.
        assert_eq!(&ids[..2], [ids::GRID_MAKER, ids::CMDNC_ARB]);
        for worker_id in WORKER_STRATEGY_IDS {
            assert!(
                ids.contains(&worker_id),
                "worker strategy {worker_id:?} must be visible to the UI"
            );
        }
        assert_eq!(ids.len(), 2 + WORKER_STRATEGY_IDS.len());
    }

    #[test]
    fn every_descriptor_is_builtin() {
        for d in builtin_descriptors() {
            assert!(d.builtin, "{} must be builtin", d.id);
            for (i, p) in d.params.iter().enumerate() {
                assert!(!p.label.is_empty(), "{}.params[{i}].label empty", d.id);
                assert!(!p.dimension.is_empty(), "{}.params[{i}].dimension empty", d.id);
            }
        }
        // The two detailed templates carry numeric slots; worker stubs carry
        // none (string-only configs render as TOML until a schema lands).
        let detailed: Vec<_> =
            builtin_descriptors().into_iter().filter(|d| !d.params.is_empty()).collect();
        assert_eq!(detailed.len(), 2);
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

    // ---- numeric payload invariants --------------------------------------------

    #[test]
    fn slot_min_never_exceeds_default() {
        for_each_slot(|d, p| {
            if let (Some(min), Some(default)) = (p.min, p.default) {
                assert!(min <= default, "{}.{}: min {min} > default {default}", d.id, p.label);
            }
        });
    }

    #[test]
    fn slot_default_never_exceeds_max() {
        for_each_slot(|d, p| {
            if let (Some(default), Some(max)) = (p.default, p.max) {
                assert!(default <= max, "{}.{}: default {default} > max {max}", d.id, p.label);
            }
        });
    }

    #[test]
    fn fully_bounded_slots_have_a_strictly_increasing_range() {
        // Every slot in the catalog leaves `max` unset today, so this guard has no
        // live witness; the matching proptest below drives the predicate directly
        // over generated bounded slots.
        for_each_slot(|d, p| {
            if let (Some(min), Some(max)) = (p.min, p.max) {
                assert!(
                    min < max,
                    "{}.{}: min {min} must be strictly below max {max}",
                    d.id,
                    p.label
                );
            }
        });
    }

    #[test]
    fn every_present_step_is_positive_and_finite() {
        for_each_slot(|d, p| {
            if let Some(step) = p.step {
                assert!(step.is_finite(), "{}.{}: step must be finite", d.id, p.label);
                assert!(step > 0.0, "{}.{}: step {step} must be > 0", d.id, p.label);
            }
        });
    }

    #[test]
    fn no_slot_carries_nan_or_infinity() {
        for_each_slot(|d, p| {
            assert_finite_field(d, p, "min", p.min);
            assert_finite_field(d, p, "max", p.max);
            assert_finite_field(d, p, "step", p.step);
            assert_finite_field(d, p, "default", p.default);
        });
    }

    #[test]
    fn every_slot_numeric_payload_is_finite() {
        for_each_slot(|d, p| {
            assert!(numerics_are_finite(p), "{}.{}: non-finite payload {p:?}", d.id, p.label);
        });
    }

    // ---- documented vocabularies ------------------------------------------------

    #[test]
    fn transcribed_vocabularies_are_non_empty_and_deduplicated() {
        // Guards the vocabulary sets themselves: a duplicated or emptied entry would
        // silently weaken every membership assertion below.
        assert!(!DIMENSIONS.is_empty());
        assert!(!KINDS.is_empty());
        let mut dims = DIMENSIONS.to_vec();
        dims.sort_unstable();
        let dim_total = dims.len();
        dims.dedup();
        assert_eq!(dims.len(), dim_total, "DIMENSIONS must not contain duplicates");
        let mut kinds = KINDS.to_vec();
        kinds.sort_unstable();
        let kind_total = kinds.len();
        kinds.dedup();
        assert_eq!(kinds.len(), kind_total, "KINDS must not contain duplicates");
    }

    #[test]
    fn every_slot_dimension_is_in_the_documented_vocabulary() {
        for_each_slot(|d, p| {
            assert!(
                dimension_is_documented(&p.dimension),
                "{}.{}: dimension {:?} is not one of {DIMENSIONS:?}",
                d.id,
                p.label,
                p.dimension
            );
        });
    }

    #[test]
    fn every_descriptor_kind_is_in_the_documented_vocabulary() {
        for d in builtin_descriptors() {
            assert!(
                kind_is_documented(&d.kind),
                "{}: kind {:?} is not one of {KINDS:?}",
                d.id,
                d.kind
            );
        }
    }

    #[test]
    fn worker_stubs_carry_their_strategy_kind() {
        // `kind` drives UI grouping/filtering: an arb strategy labeled
        // "maker" would vanish under the "arb" filter.
        for id in ARB_STRATEGY_IDS {
            assert_eq!(descriptor(id).expect("worker stub must exist").kind, "arb", "{id}");
        }
        for id in HEDGE_STRATEGY_IDS {
            assert_eq!(descriptor(id).expect("worker stub must exist").kind, "hedge", "{id}");
        }
        for id in WORKER_STRATEGY_IDS {
            if !ARB_STRATEGY_IDS.contains(&id) && !HEDGE_STRATEGY_IDS.contains(&id) {
                assert_eq!(descriptor(id).expect("worker stub must exist").kind, "maker", "{id}");
            }
        }
    }

    // ---- descriptor identity ----------------------------------------------------

    /// Every descriptor must be displayable: the UI renders the id, name, and
    /// note with no fallback text, so an empty one would ship a blank card.
    #[test]
    fn every_descriptor_has_displayable_identity() {
        for d in builtin_descriptors() {
            assert!(!d.id.is_empty(), "descriptor id must not be empty");
            assert!(!d.name.is_empty(), "{}.name must not be empty", d.id);
            assert!(!d.note.is_empty(), "{}.note must not be empty", d.id);
        }
    }

    /// Detailed templates fully specify their controls: every numeric slot
    /// declares an explicit step. A missing step would silently fall back to
    /// the dimension default and change the UI granularity.
    #[test]
    fn detailed_descriptors_declare_explicit_steps() {
        for d in [grid_maker(), cmdnc_arb()] {
            assert!(!d.params.is_empty(), "{}.params must not be empty", d.id);
            for p in &d.params {
                assert!(
                    p.step.is_some(),
                    "{}.{}: detailed slots must declare an explicit step",
                    d.id,
                    p.label
                );
            }
        }
    }

    #[test]
    fn descriptor_ids_are_unique_non_empty_and_snake_case() {
        let descriptors = builtin_descriptors();
        let mut seen = BTreeSet::new();
        for d in &descriptors {
            assert!(!d.id.is_empty(), "descriptor id must not be empty");
            assert!(id_is_snake_case(&d.id), "descriptor id {:?} is not snake_case", d.id);
            assert!(seen.insert(d.id.as_str()), "duplicate descriptor id {:?}", d.id);
        }
        assert_eq!(seen.len(), descriptors.len());
    }

    #[test]
    fn param_labels_are_unique_and_non_empty_per_descriptor() {
        for d in builtin_descriptors() {
            let mut labels = BTreeSet::new();
            for p in &d.params {
                assert!(!p.label.is_empty(), "{}: param label must not be empty", d.id);
                assert!(
                    labels.insert(p.label.as_str()),
                    "{}: duplicate param label {:?}",
                    d.id,
                    p.label
                );
            }
        }
    }

    // ---- lookup totality and case sensitivity -----------------------------------

    #[test]
    fn descriptor_lookup_is_total_over_the_catalog() {
        for d in builtin_descriptors() {
            let found = descriptor(&d.id).expect("every builtin id must resolve");
            assert_eq!(found.id, d.id);
            assert_eq!(found.params, d.params);
        }
    }

    #[test]
    fn descriptor_lookup_is_case_sensitive() {
        for d in builtin_descriptors() {
            let shouty = d.id.to_ascii_uppercase();
            assert_ne!(shouty, d.id);
            assert!(descriptor(&shouty).is_none(), "{shouty} must not resolve");
        }
        // Neither the display name nor a spaced / mixed-case variant is an alias.
        assert!(descriptor("Grid Maker").is_none());
        assert!(descriptor("grid maker").is_none());
        assert!(descriptor("Grid_Maker").is_none());
        assert!(descriptor("GRID_MAKER").is_none());
        assert!(descriptor("").is_none());
    }

    #[test]
    fn builtin_descriptors_is_deterministic() {
        let first = builtin_descriptors();
        let second = builtin_descriptors();
        assert_eq!(first, second, "builtin_descriptors() must be a pure function");
        // 2 detailed (6 + 4 numeric slots) + 55 worker stubs (no numeric slots).
        let counts: Vec<usize> = first.iter().map(|d| d.params.len()).collect();
        assert_eq!(&counts[..2], [6, 4]);
        assert!(counts[2..].iter().all(|c| *c == 0));
        assert_eq!(all_slots().len(), 10);
        assert_eq!(first.len(), 2 + WORKER_STRATEGY_IDS.len());
    }

    // ---- property-based invariants ----------------------------------------------

    proptest! {
        // No catalog slot is bounded on both ends today, so drive the strict-range
        // predicate directly over arbitrary finite triples. This proves the predicate
        // is neither vacuously true nor vacuously false.
        #[test]
        fn strict_range_predicate_tracks_numeric_ordering(
            min in arb_finite_f64(),
            max in arb_finite_f64(),
            default in arb_finite_f64(),
        ) {
            let bounded = slot("x", "usd", Some(min), Some(max), Some(1.0), Some(default));
            prop_assert_eq!(strict_min_lt_max(&bounded), min < max);
            prop_assert_eq!(min_le_default(&bounded), min <= default);
            prop_assert_eq!(default_le_max(&bounded), default <= max);

            // An unbounded slot is vacuously well-ordered.
            let unbounded = slot("x", "usd", Some(min), None, Some(1.0), Some(default));
            prop_assert!(strict_min_lt_max(&unbounded));
        }
    }

    proptest! {
        // The catalog-wide bound predicate must agree with plain numeric ordering and
        // must fire on every out-of-range construction, so the sweep tests above can
        // never silently degrade into tautologies.
        #[test]
        fn bounds_predicate_detects_out_of_range_slots(
            min in arb_finite_f64(),
            max in arb_finite_f64(),
            default in arb_finite_f64(),
        ) {
            let s = slot("x", "usd", Some(min), Some(max), Some(1.0), Some(default));
            let swapped = slot("x", "usd", Some(max), Some(min), Some(1.0), Some(default));
            prop_assert_eq!(strict_min_lt_max(&s), min < max);
            prop_assert_eq!(strict_min_lt_max(&swapped), max < min);
            prop_assert_eq!(min_le_default(&swapped), max <= default);
            prop_assert_eq!(default_le_max(&swapped), default <= min);

            // A default outside a well-ordered range is always caught by one side.
            if min < max && !(min <= default && default <= max) {
                prop_assert!(!min_le_default(&s) || !default_le_max(&s));
            }
        }
    }

    proptest! {
        // Re-checks the catalog-wide invariants through the proptest harness by
        // sampling slots, so every declared slot is re-visited on every run.
        #[test]
        fn sampled_catalog_slots_satisfy_every_invariant(
            sampled in prop::sample::select(all_slots()),
        ) {
            prop_assert!(!sampled.label.is_empty());
            prop_assert!(min_le_default(&sampled), "{}: min > default", sampled.label);
            prop_assert!(default_le_max(&sampled), "{}: default > max", sampled.label);
            prop_assert!(strict_min_lt_max(&sampled), "{}: range not strict", sampled.label);
            prop_assert!(step_is_positive_finite(&sampled), "{}: bad step", sampled.label);
            prop_assert!(numerics_are_finite(&sampled), "{}: non-finite", sampled.label);
            prop_assert!(
                dimension_is_documented(&sampled.dimension),
                "{}: undocumented dimension",
                sampled.dimension
            );
        }
    }
}

/// The README strategy tables are the catalogue a reader sees;
/// [`WORKER_STRATEGY_IDS`] is the catalogue the worker can actually build.
///
/// The two drifted once — the tables listed 22 of 55 registered strategies, so
/// a reader comparing frameworks counted a third of the strategy library — and
/// nothing in the build noticed, because the tables are prose. The set equality
/// is pinned here instead of left to review.
#[cfg(test)]
mod readme_sync_tests {
    use std::{collections::BTreeSet, path::PathBuf};

    use super::WORKER_STRATEGY_IDS;

    /// Table rows of the `##` section whose heading contains `marker`.
    ///
    /// Only table rows are collected: a fenced ```` ```toml ```` block also
    /// contains backticks, and counting those would make the extraction depend
    /// on fence parity rather than on what the table actually lists.
    fn strategy_table_rows<'a>(markdown: &'a str, marker: &str) -> Vec<&'a str> {
        let mut lines =
            markdown.lines().skip_while(|l| !(l.starts_with("## ") && l.contains(marker)));
        assert!(lines.next().is_some(), "no `##` heading containing {marker:?}");
        lines
            .take_while(|l| !l.starts_with("## "))
            .filter(|l| l.trim_start().starts_with('|'))
            .collect()
    }

    /// Every `snake_case` identifier the section's tables mention in backticks.
    fn documented_ids(markdown: &str, marker: &str) -> BTreeSet<String> {
        let mut ids = BTreeSet::new();
        for row in strategy_table_rows(markdown, marker) {
            for span in row.split('`').skip(1).step_by(2) {
                if is_snake_case_id(span) {
                    ids.insert(String::from(span));
                }
            }
        }
        ids
    }

    /// `snake_case`, the shape every strategy id has; anything else in backticks
    /// is prose (`ports::TradingGateway`, `strategy.type`, a path) and is not a
    /// strategy claim.
    fn is_snake_case_id(id: &str) -> bool {
        let Some(first) = id.chars().next() else { return false };
        first.is_ascii_lowercase() &&
            id.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    }

    /// Read at test time, not `include_str!`: a build-time embedding is not
    /// re-run when only the README changes, so the guard would keep passing
    /// against a stale copy.
    fn readme(name: &str) -> String {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join(name);
        std::fs::read_to_string(&path).expect("the README must be readable")
    }

    fn registered_ids() -> BTreeSet<String> {
        WORKER_STRATEGY_IDS.iter().map(|id| String::from(*id)).collect()
    }

    #[test]
    fn the_english_readme_names_exactly_the_registered_strategies() {
        let documented = documented_ids(&readme("README.md"), "Strategies");
        let registered = registered_ids();
        assert!(!documented.is_empty(), "the README strategy table must not be empty");
        assert_eq!(
            documented, registered,
            "README.md's strategy table must list exactly the registered strategies"
        );
    }

    /// The Chinese README is a translation, not a summary: a reader who
    /// switches language must not see a different strategy library.
    #[test]
    fn the_chinese_readme_names_exactly_the_registered_strategies() {
        let documented = documented_ids(&readme("README.zh.md"), "策略");
        let registered = registered_ids();
        assert!(!documented.is_empty(), "the README strategy table must not be empty");
        assert_eq!(
            documented, registered,
            "README.zh.md's strategy table must list exactly the registered strategies"
        );
    }

    /// Both READMEs must agree with each other, so the two tests above cannot
    /// pass by each matching a different half of the registry.
    #[test]
    fn the_two_readmes_name_the_same_strategies() {
        assert_eq!(
            documented_ids(&readme("README.md"), "Strategies"),
            documented_ids(&readme("README.zh.md"), "策略"),
            "README.md and README.zh.md must list the same strategies"
        );
    }
}
