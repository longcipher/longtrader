//! State-file persistence for [`super::SimpleGrid`] ladders.
//!
//! The ladder is the strategy's only durable state: after a restart it must
//! resume what the previous process left resting instead of seeding a second
//! ladder on top of it. That makes the restore path fail-loud — a corrupt file
//! stops the grid rather than silently double-quoting — and the save path
//! fail-quiet: a ladder the venue still holds is worth more than the file that
//! failed to record it.

use rust_decimal::Decimal;

use super::{GridLevel, SimpleGridConfig};
use crate::proto::trading;

/// Writes the ladder when `state_file` is configured; a failed write is
/// logged and otherwise ignored (see the module docs).
pub(super) fn persist_levels(state_file: Option<&str>, levels: &[GridLevel]) {
    let Some(path) = state_file else {
        return;
    };
    let store = crate::state_store::StateStore::new(path);
    let json = serde_json::json!(
        levels
            .iter()
            .map(|l| serde_json::json!({
                "price": l.price.to_string(),
                "side": if matches!(l.side, trading::OrderSide::Buy) { "buy" } else { "sell" },
                "client_order_id": l.client_order_id,
                // The venue order id travels with the level so a restored
                // ladder can still be addressed by the kill-switch. It is
                // only a hint: an id the venue has since retired fails the
                // cancel, which the kill-switch already treats as a warning.
                "order_id": l.order_id,
            }))
            .collect::<Vec<_>>()
    );
    let _ = store.save(&json).inspect_err(|err| {
        tracing::warn!(error = %err, "grid state save failed");
    });
}

/// Reads the ladder back.
///
/// An empty vector means "nothing to restore": no file configured, no file
/// present, or a file holding an empty ladder — which is exactly what the
/// kill-switch writes, and what the in-memory path does with an empty ladder
/// anyway.
///
/// A read or parse failure is an error rather than a silent seed-from-scratch.
/// The seeding path quotes every level without reading the venue, so a
/// restored-and-then-dropped ladder leaves the previous process's orders
/// resting *and* a second ladder on top of them — twice the intended
/// inventory, with the flip logic unable to tell the copies apart. Refusing
/// the cycle is loud and costs no money; guessing is silent and does.
///
/// # Errors
///
/// Propagates read/parse failures, including a document that is not a list of
/// level rows.
pub(super) fn restore_levels(config: &SimpleGridConfig) -> color_eyre::Result<Vec<GridLevel>> {
    let Some(path) = config.state_file.as_deref() else {
        return Ok(Vec::new());
    };
    let Some(document) = crate::state_store::StateStore::new(path).load()? else {
        return Ok(Vec::new());
    };
    let rows = document
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("grid state at {path} is not a list of levels"))?;
    let mut levels = Vec::with_capacity(rows.len());
    let mut off_band = 0usize;
    for row in rows {
        let level = level_from_json(row)?;
        // A level the current band no longer contains can neither be
        // re-quoted nor flipped sensibly, so it is dropped rather than left to
        // linger as a level the strategy can never act on.
        if level.price < config.lower_price || level.price > config.upper_price {
            off_band += 1;
            continue;
        }
        levels.push(level);
    }
    if off_band > 0 {
        tracing::warn!(off_band, "dropped restored grid levels outside the configured band");
    }
    Ok(levels)
}

/// One persisted level row.
///
/// A row the strategy wrote always carries a price and a side. A missing
/// client order id is replaced with a fresh one rather than refused: a
/// restored level is matched against the venue by price, and it is given a new
/// id the moment it is flipped anyway.
fn level_from_json(row: &serde_json::Value) -> color_eyre::Result<GridLevel> {
    let price = row
        .get("price")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| color_eyre::eyre::eyre!("grid level row has no price"))?
        .parse::<Decimal>()
        .map_err(|err| color_eyre::eyre::eyre!("grid level price is not a decimal: {err}"))?;
    let side = match row.get("side").and_then(serde_json::Value::as_str) {
        Some("buy") => trading::OrderSide::Buy,
        Some("sell") => trading::OrderSide::Sell,
        other => {
            return Err(color_eyre::eyre::eyre!("grid level side is not buy/sell: {other:?}"));
        }
    };
    let text = |key: &str| {
        row.get(key).and_then(serde_json::Value::as_str).unwrap_or_default().to_string()
    };
    let stored_coid = text("client_order_id");
    let client_order_id = if stored_coid.is_empty() {
        format!("grid-{}", ulid::Ulid::generate())
    } else {
        stored_coid
    };
    Ok(GridLevel { price, side, client_order_id, order_id: text("order_id") })
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn level(price: Decimal, side: trading::OrderSide, coid: &str, order_id: &str) -> GridLevel {
        GridLevel { price, side, client_order_id: coid.to_string(), order_id: order_id.to_string() }
    }

    #[test]
    fn a_row_round_trips_price_side_and_ids() {
        let row = serde_json::json!({
            "price": "95.5",
            "side": "buy",
            "client_order_id": "grid-1",
            "order_id": "venue-9",
        });
        let restored = level_from_json(&row).expect("row");
        assert_eq!(restored.price, dec!(95.5));
        assert_eq!(restored.side, trading::OrderSide::Buy);
        assert_eq!(restored.client_order_id, "grid-1");
        assert_eq!(restored.order_id, "venue-9");
    }

    #[test]
    fn a_row_without_a_client_order_id_gets_a_fresh_one() {
        let row = serde_json::json!({ "price": "95.5", "side": "sell" });
        let restored = level_from_json(&row).expect("row");
        assert!(
            restored.client_order_id.starts_with("grid-"),
            "a fresh id must be minted, got {:?}",
            restored.client_order_id
        );
        assert_eq!(restored.order_id, "", "an absent order id stays empty");
    }

    #[test]
    fn a_row_without_a_price_is_an_error() {
        let row = serde_json::json!({ "side": "buy" });
        assert!(level_from_json(&row).is_err());
    }

    #[test]
    fn a_row_with_an_unknown_side_is_an_error() {
        let row = serde_json::json!({ "price": "95.5", "side": "sideways" });
        let err = level_from_json(&row).expect_err("unknown side");
        assert!(err.to_string().contains("sideways"), "got {err}");
    }

    #[test]
    fn a_row_with_a_non_decimal_price_is_an_error() {
        let row = serde_json::json!({ "price": "not-a-number", "side": "buy" });
        assert!(level_from_json(&row).is_err());
    }

    #[test]
    fn persisting_without_a_state_file_is_a_no_op() {
        // Must not panic and must not touch the filesystem.
        persist_levels(None, &[level(dec!(95), trading::OrderSide::Buy, "c", "o")]);
    }
}
