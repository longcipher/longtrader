use clap::ValueEnum;
use longtrader_proto::proto::longtrader::{
    common::v1 as common, market::v1 as umarket, terminal::v1 as proto, trading::v1 as utrading,
};

#[derive(Debug, Clone, ValueEnum)]
pub(crate) enum OutputFormat {
    Table,
    Json,
}

/// Render a contract decimal for the table view.
///
/// The payload is the only representation there is, so a value that decodes
/// renders as the number itself and there is no second copy to fall back on.
/// Every way a payload can fail to decode — a blank one a writer never filled
/// in, garbage outside the contract grammar, a well-formed number too wide for
/// a decimal — therefore has to be reported rather than printed, because a
/// blank cell is indistinguishable from a value that was never sent.
pub(crate) fn fmt_dec(d: &common::Decimal) -> String {
    match longtrader_contract::ext::common_to_decimal(d) {
        Ok(value) => value.to_string(),
        Err(err) => format!("<undecodable: {err}>"),
    }
}

pub(crate) struct Renderer {
    pub(crate) format: OutputFormat,
}

impl Renderer {
    pub(crate) fn new(format: OutputFormat) -> Self {
        Self { format }
    }

    /// Left-align and truncate `s` to `width` columns, appending an ellipsis
    /// when it overflows. Keeps table output aligned regardless of payload.
    fn trunc(s: &str, width: usize) -> String {
        let chars: Vec<char> = s.chars().collect();
        if chars.len() <= width {
            format!("{s:<width$}")
        } else {
            let head: String = chars.iter().take(width.saturating_sub(1)).collect();
            format!("{head}…")
        }
    }

    pub(crate) fn render_symbols(&self, symbols: &[umarket::SymbolInfo]) {
        match self.format {
            OutputFormat::Json => match serde_json::to_string_pretty(symbols) {
                Ok(json) => println!("{json}"),
                Err(e) => eprintln!("JSON serialization error: {e}"),
            },
            OutputFormat::Table => {
                println!("{:<15} {:<20} {:<10} {:<10}", "NAME", "DISPLAY", "BASE", "QUOTE");
                println!("{}", "-".repeat(60));
                for s in symbols {
                    println!(
                        "{:<15} {:<20} {:<10} {:<10}",
                        Self::trunc(&s.name, 15),
                        Self::trunc(&s.display_name, 20),
                        Self::trunc(&s.base_asset, 10),
                        Self::trunc(&s.quote_asset, 10),
                    );
                }
            }
        }
    }

    pub(crate) fn render_candles(&self, candles: &[umarket::Candle]) {
        match self.format {
            OutputFormat::Json => match serde_json::to_string_pretty(candles) {
                Ok(json) => println!("{json}"),
                Err(e) => eprintln!("JSON serialization error: {e}"),
            },
            OutputFormat::Table => {
                println!(
                    "{:<20} {:<12} {:<12} {:<12} {:<12} {:<12}",
                    "TIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"
                );
                println!("{}", "-".repeat(80));
                for c in candles {
                    println!(
                        "{:<20} {:<12} {:<12} {:<12} {:<12} {:<12}",
                        c.timestamp_ms,
                        fmt_dec(&c.open),
                        fmt_dec(&c.high),
                        fmt_dec(&c.low),
                        fmt_dec(&c.close),
                        fmt_dec(&c.volume)
                    );
                }
            }
        }
    }

    pub(crate) fn render_account(&self, account: &utrading::Account) {
        match self.format {
            OutputFormat::Json => match serde_json::to_string_pretty(account) {
                Ok(json) => println!("{json}"),
                Err(e) => eprintln!("JSON serialization error: {e}"),
            },
            OutputFormat::Table => {
                println!("{:<15} {}", "Balance", fmt_dec(&account.balance));
                println!("{:<15} {}", "Equity", fmt_dec(&account.equity));
                println!("{:<15} {}", "Margin Used", fmt_dec(&account.margin_used));
                println!("{:<15} {}", "Free Margin", fmt_dec(&account.free_margin));
                println!("{:<15} {}", "Margin Frozen", fmt_dec(&account.margin_frozen));
            }
        }
    }

    pub(crate) fn render_positions(&self, positions: &[utrading::Position]) {
        match self.format {
            OutputFormat::Json => match serde_json::to_string_pretty(positions) {
                Ok(json) => println!("{json}"),
                Err(e) => eprintln!("JSON serialization error: {e}"),
            },
            OutputFormat::Table => {
                println!(
                    "{:<12} {:<12} {:<8} {:<12} {:<12} {:<12} {:<12}",
                    "ID", "SYMBOL", "SIDE", "QTY", "ENTRY", "CURRENT", "PnL"
                );
                println!("{}", "-".repeat(80));
                for p in positions {
                    // `trading.v1.Position.side` is an `OrderSide`: a long
                    // position is the one bought first, a short the one sold.
                    let side = match p.side {
                        buffa::EnumValue::Known(utrading::OrderSide::Buy) => "Long",
                        buffa::EnumValue::Known(utrading::OrderSide::Sell) => "Short",
                        _ => "-",
                    };
                    println!(
                        "{:<12} {:<12} {:<8} {:<12} {:<12} {:<12} {:<12}",
                        Self::trunc(&p.id, 12),
                        Self::trunc(&p.symbol, 12),
                        side,
                        fmt_dec(&p.contracts),
                        fmt_dec(&p.entry_price),
                        fmt_dec(&p.current_price),
                        fmt_dec(&p.unrealized_pnl)
                    );
                }
            }
        }
    }

    pub(crate) fn render_orders(&self, orders: &[utrading::Order]) {
        match self.format {
            OutputFormat::Json => match serde_json::to_string_pretty(orders) {
                Ok(json) => println!("{json}"),
                Err(e) => eprintln!("JSON serialization error: {e}"),
            },
            OutputFormat::Table => {
                println!(
                    "{:<12} {:<12} {:<6} {:<8} {:<10} {:<10} {:<10}",
                    "ID", "SYMBOL", "SIDE", "TYPE", "QTY", "PRICE", "STATUS"
                );
                println!("{}", "-".repeat(70));
                for o in orders {
                    let side = match o.side {
                        buffa::EnumValue::Known(utrading::OrderSide::Buy) => "Buy",
                        buffa::EnumValue::Known(utrading::OrderSide::Sell) => "Sell",
                        _ => "-",
                    };
                    let otype = match o.r#type {
                        buffa::EnumValue::Known(utrading::OrderType::Market) => "Market",
                        buffa::EnumValue::Known(utrading::OrderType::Limit) => "Limit",
                        buffa::EnumValue::Known(utrading::OrderType::Stop) => "Stop",
                        buffa::EnumValue::Known(utrading::OrderType::StopLimit) => "StopLimit",
                        _ => "-",
                    };
                    let status = match o.status {
                        buffa::EnumValue::Known(utrading::OrderStatus::Open) => "Open",
                        buffa::EnumValue::Known(utrading::OrderStatus::Filled) => "Filled",
                        buffa::EnumValue::Known(utrading::OrderStatus::Canceled) => "Canceled",
                        buffa::EnumValue::Known(utrading::OrderStatus::Rejected) => "Rejected",
                        _ => "-",
                    };
                    println!(
                        "{:<12} {:<12} {:<6} {:<8} {:<10} {:<10} {:<10}",
                        Self::trunc(&o.id, 12),
                        Self::trunc(&o.symbol, 12),
                        side,
                        otype,
                        fmt_dec(&o.amount),
                        fmt_dec(&o.price),
                        status
                    );
                }
            }
        }
    }

    pub(crate) fn render_venues(&self, venues: &[proto::VenueStatus]) {
        match self.format {
            OutputFormat::Json => match serde_json::to_string_pretty(venues) {
                Ok(json) => println!("{json}"),
                Err(e) => eprintln!("JSON serialization error: {e}"),
            },
            OutputFormat::Table => {
                println!("{:<15} {:<10} {:<10}", "NAME", "CONNECTED", "SYMBOLS");
                println!("{}", "-".repeat(35));
                for v in venues {
                    println!(
                        "{:<15} {:<10} {:<10}",
                        Self::trunc(&v.name, 15),
                        v.connected,
                        v.symbol_count
                    );
                }
            }
        }
    }

    pub(crate) fn render_msg(&self, msg: &str) {
        let _ = self;
        println!("{msg}");
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// A wire decimal for `mantissa * 10^-scale`, spelled the way every other
    /// fixture spells it. Going through the encoder keeps these fixtures
    /// identical to what a real writer emits.
    fn dec(mantissa: i64, scale: i32) -> common::Decimal {
        let scale = u32::try_from(scale).expect("these fixtures never use a negative scale");
        longtrader_contract::ext::decimal_to_common(rust_decimal::Decimal::new(mantissa, scale))
    }

    /// A wire decimal carrying exactly the payload `value`. Every shape a
    /// writer never produces is expressible here, which is what the error-path
    /// tests need.
    fn payload(value: &str) -> common::Decimal {
        common::Decimal { value: value.to_string(), ..Default::default() }
    }

    fn table() -> Renderer {
        Renderer::new(OutputFormat::Table)
    }

    fn json() -> Renderer {
        Renderer::new(OutputFormat::Json)
    }

    /// The plain-decimal rendering of `Decimal::new(1, scale)` with every
    /// trailing zero intact: never scientific notation, always the full scale.
    fn unit_fraction(scale: usize) -> String {
        format!("0.{}{}", "0".repeat(scale - 1), "1")
    }

    // ---- fmt_dec: a payload that decodes ----

    #[test]
    fn a_decimal_renders_as_the_number_the_writer_sent() {
        assert_eq!(fmt_dec(&dec(12_345, 2)), "123.45");
    }

    #[test]
    fn trailing_zeros_in_the_value_are_preserved() {
        assert_eq!(fmt_dec(&dec(100, 4)), "0.0100");
        assert_eq!(fmt_dec(&dec(0, 3)), "0.000");
    }

    #[test]
    fn a_negative_mantissa_renders_with_its_sign() {
        assert_eq!(fmt_dec(&dec(-1, 8)), "-0.00000001");
    }

    #[test]
    fn the_largest_i64_mantissa_renders_exactly() {
        assert_eq!(fmt_dec(&dec(i64::MAX, 0)), "9223372036854775807");
    }

    /// A payload wider than an `i64` used to need a second representation,
    /// because the numeric pair could not hold it. It has none now, so the value
    /// text is all there is — and it still decodes.
    #[test]
    fn a_value_wider_than_i64_is_decoded_and_rendered_numerically() {
        assert_eq!(fmt_dec(&payload("9223372036854775808")), "9223372036854775808");
        assert_eq!(fmt_dec(&payload(&unit_fraction(28))), unit_fraction(28));
    }

    #[test]
    fn the_widest_representable_decimal_renders_exactly() {
        assert_eq!(
            fmt_dec(&payload("79228162514264337593543950335")),
            "79228162514264337593543950335"
        );
    }

    #[test]
    fn a_whole_number_renders_without_a_decimal_point() {
        // What a scaled-up mantissa used to be spelled. The contract carries the
        // value alone, so magnitude above one is just more digits.
        assert_eq!(fmt_dec(&payload("100000")), "100000");
        assert_eq!(fmt_dec(&payload("-700")), "-700");
        assert_eq!(fmt_dec(&payload("5")), "5", "a unit value is unchanged");
    }

    // ---- fmt_dec: an undecodable payload must be visible, not blank ----
    //
    // The payload is the only representation, so there is nothing left to print
    // when it will not decode: a blank payload a writer never filled in, garbage
    // the grammar refuses, and a well-formed number too wide for a decimal all
    // have to render a visible marker. A blank cell is indistinguishable from a
    // value that was never sent.

    /// The all-defaults message is what an unpopulated `Decimal` looks like, so
    /// it must read as an error rather than as a zero an operator would trust.
    #[test]
    fn an_unpopulated_payload_renders_as_a_visible_error_not_a_zero() {
        let rendered = fmt_dec(&common::Decimal::default());
        assert!(rendered.starts_with("<undecodable:"), "got {rendered:?}");
        assert_eq!(fmt_dec(&dec(0, 0)), "0", "an encoded zero is still a zero");
    }

    #[test]
    fn the_maximum_fractional_scale_decodes_but_one_more_digit_does_not() {
        // 28 is `rust_decimal`'s maximum scale. A 29th digit would be rounded
        // away, so the contract grammar refuses the payload rather than letting
        // the table show a value the wire never carried.
        assert_eq!(fmt_dec(&payload(&unit_fraction(28))), unit_fraction(28));
        let rendered = fmt_dec(&payload(&unit_fraction(29)));
        assert!(rendered.starts_with("<undecodable:"), "got {rendered:?}");
    }

    #[test]
    fn a_well_formed_value_too_wide_for_a_decimal_renders_as_a_visible_error() {
        // Both payloads are inside the grammar, so only the range check can
        // reject them: 10^29 and 2^96 each overshoot the 96-bit coefficient.
        for literal in ["100000000000000000000000000000", "79228162514264337593543950336"] {
            let rendered = fmt_dec(&payload(literal));
            assert!(rendered.starts_with("<undecodable:"), "{literal:?} rendered {rendered:?}");
        }
    }

    #[test]
    fn a_payload_outside_the_contract_grammar_renders_as_a_visible_error() {
        for literal in ["1_000", "1e3", "1E3", "+1", ".5", "1.", "1,000", " 1 ", "1 "] {
            let rendered = fmt_dec(&payload(literal));
            assert!(rendered.starts_with("<undecodable:"), "{literal:?} rendered {rendered:?}");
        }
    }

    // ---- Renderer::trunc: padding branch ----

    #[test]
    fn a_short_string_is_padded_on_the_right_up_to_the_column_width() {
        assert_eq!(Renderer::trunc("abc", 5), "abc  ");
    }

    #[test]
    fn a_string_of_exactly_the_column_width_is_returned_untouched() {
        assert_eq!(Renderer::trunc("abcde", 5), "abcde");
    }

    #[test]
    fn an_empty_string_pads_out_to_the_column_width() {
        assert_eq!(Renderer::trunc("", 3), "   ");
    }

    #[test]
    fn an_empty_string_at_width_zero_is_empty() {
        assert_eq!(Renderer::trunc("", 0), "");
    }

    // ---- Renderer::trunc: ellipsis branch ----

    #[test]
    fn an_overlong_string_gives_up_its_last_column_to_an_ellipsis() {
        assert_eq!(Renderer::trunc("abcdef", 3), "ab…");
    }

    #[test]
    fn a_string_exactly_one_column_over_the_width_keeps_one_real_column() {
        // 4 chars into 3 columns: two real chars survive plus the marker.
        assert_eq!(Renderer::trunc("abcd", 3), "ab…");
    }

    #[test]
    fn a_width_of_one_leaves_room_for_the_ellipsis_only() {
        assert_eq!(Renderer::trunc("abcdef", 1), "…");
    }

    // ---- Renderer::trunc: PINNED DEFECT, width zero overflows ----
    //
    // `width.saturating_sub(1)` makes the head empty at `width == 0`, but the
    // ellipsis is still appended unconditionally, so a non-empty string comes
    // back one column WIDER than the caller asked for. `trunc` is only ever
    // called with widths of 6..=20 today, so this cannot corrupt a live table,
    // but the function does not honour its own contract at width 0.

    #[test]
    fn a_zero_width_on_a_non_empty_string_yields_a_lone_ellipsis() {
        let out = Renderer::trunc("x", 0);
        assert_eq!(out, "…");
        assert_eq!(out.chars().count(), 1, "one column was produced for a zero-column request");
    }

    #[test]
    fn a_zero_width_does_not_truncate_a_longer_string_either() {
        assert_eq!(Renderer::trunc("abcdef", 0), "…");
    }

    // ---- Renderer::trunc: PINNED DEFECT, scalars are not display columns ----
    //
    // Both branches of `trunc` count `char`s (Unicode scalars), so wide and
    // combining glyphs break the "width columns" contract in both directions:
    // a wide glyph eats two columns per scalar (overflow), and a base character
    // plus its combining mark is two scalars for one column (over-truncation).

    #[test]
    fn truncation_counts_unicode_scalars_so_wide_glyphs_overflow_the_column_budget() {
        // Three scalars into a two-column budget yields "日…" = two scalars but
        // three display columns.
        let out = Renderer::trunc("日本語", 2);
        assert_eq!(out, "日…");
        assert_eq!(out.chars().count(), 2);
    }

    #[test]
    fn padding_counts_unicode_scalars_so_wide_glyphs_underflow_the_column_budget() {
        // "日本" is two scalars but four display columns; padding to 4 columns
        // leaves the cell two columns short of the promised width.
        assert_eq!(Renderer::trunc("日本", 4), "日本  ");
    }

    #[test]
    fn truncation_can_destroy_a_whole_combining_grapheme() {
        // "e" + COMBINING ACUTE ACCENT is one printed column but two scalars,
        // so a one-column budget throws the entire glyph away.
        let out = Renderer::trunc("e\u{0301}", 1);
        assert_eq!(out, "…");
        assert!(!out.contains('e'), "the base character was dropped entirely");
    }

    // ---- Renderer::trunc: the shape every caller relies on ----

    #[test]
    fn truncation_always_fills_exactly_the_requested_width_for_widths_of_at_least_one() {
        for width in 1..=24usize {
            for input in ["", "a", "abc", "abcdefghij", "日本語", "e\u{0301}x"] {
                let out = Renderer::trunc(input, width);
                assert_eq!(
                    out.chars().count(),
                    width,
                    "expected exactly {width} scalars from {input:?} at width {width}, got {out:?}"
                );
            }
        }
    }

    #[test]
    fn an_overlong_string_is_never_silently_shortened_without_the_ellipsis() {
        let out = Renderer::trunc("abcdefghij", 4);
        assert!(out.ends_with('…'), "an overflowed cell must be marked, got {out:?}");
        assert!(input_prefix_of(&out, "abcdefghij"), "the head must be an input prefix");
    }

    /// Whether `head` is a character-wise prefix of `input`, ignoring the
    /// ellipsis marker `trunc` may append and the padding it may add.
    fn input_prefix_of(head: &str, input: &str) -> bool {
        let head: Vec<char> = head.trim_end_matches(['…', ' ']).chars().collect();
        let input: Vec<char> = input.chars().collect();
        head.len() <= input.len() && head.iter().zip(&input).all(|(a, b)| a == b)
    }

    // ---- proptest: the invariants the table layout depends on ----

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// The width contract: `trunc` returns exactly `width` scalars for every
        /// width of at least one, and never more than one for width zero. It must
        /// not panic on any input either.
        #[test]
        fn trunc_never_exceeds_the_width_for_widths_of_at_least_one(
            input in ".{0,40}",
            width in 1usize..=40,
        ) {
            let out = Renderer::trunc(&input, width);
            prop_assert_eq!(out.chars().count(), width, "input={:?} width={}", input, width );
            prop_assert!(input_prefix_of(&out, &input), "head must be a prefix of {input:?}");
        }

        /// Width zero is the documented exception: a non-empty input still costs
        /// one column for the ellipsis. Pinned so a future fix shows up as a
        /// deliberate change rather than a silent behaviour shift.
        #[test]
        fn a_zero_width_costs_at_most_one_column(
            input in ".{0,20}",
            is_empty in proptest::bool::ANY,
        ) {
            let input = if is_empty { String::new() } else { input };
            let out = Renderer::trunc(&input, 0);
            prop_assert!(out.chars().count() <= 1, "input={input:?} produced {out:?}");
            // A non-empty input still fits in the zero width, so it costs the one
            // ellipsis column. An empty input has nothing to mark and stays empty.
            prop_assert_eq!(out.chars().count(), usize::from(!input.is_empty()));
        }

        /// A successful decode must render exactly as the decimal formats
        /// itself; the renderer may not reformat or re-round the value.
        #[test]
        fn a_decodable_decimal_renders_as_the_decimal_formats_itself(
            mantissa in proptest::num::i64::ANY,
            scale in 0i32..=28,
        ) {
            let wire = dec(mantissa, scale);
            let decoded = longtrader_contract::ext::common_to_decimal(&wire)
                .expect("scale 0..=28 with an i64 mantissa is always decodable");
            prop_assert_eq!(fmt_dec(&wire), decoded.to_string());
        }

        /// A blank payload renders a visible marker, never a blank cell: a
        /// writer that sends an unpopulated `Decimal` must not be
        /// indistinguishable from one that sent nothing at all.
        #[test]
        fn a_blank_payload_never_renders_a_blank_cell(blank in "[ \t]{0,8}") {
            let rendered = fmt_dec(&payload(&blank));
            prop_assert!(
                rendered.starts_with("<undecodable:"),
                "payload={:?} rendered={:?}",
                blank,
                rendered
            );
        }

        /// A payload the grammar refuses is echoed into the rendered error, so
        /// an operator can see what the peer actually sent instead of a bare
        /// "undecodable". Each noise byte below is enough on its own to break
        /// the grammar in a 1..=4 byte run.
        #[test]
        fn an_undecodable_payload_is_echoed_into_the_rendered_error(noise in "[-_eE+]{1,4}") {
            let literal = format!("1{noise}000");
            let rendered = fmt_dec(&payload(&literal));
            prop_assert!(
                rendered.starts_with("<undecodable:"),
                "payload={:?} rendered={:?}",
                literal,
                rendered
            );
            prop_assert!(
                rendered.contains(&literal),
                "payload={:?} rendered={:?}",
                literal,
                rendered
            );
        }

        /// Nothing `fmt_dec` can be handed may render as an empty cell.
        #[test]
        fn fmt_dec_never_renders_a_blank_cell(value in ".{0,16}") {
            let wire = payload(&value);
            let rendered = fmt_dec(&wire);
            prop_assert!(!rendered.is_empty(), "rendered a blank cell for {:?}", wire);
        }

        /// `fmt_dec` must never panic on any payload a peer can send.
        #[test]
        fn fmt_dec_never_panics(value in ".{0,24}") {
            let _ = fmt_dec(&payload(&value));
        }
    }

    // ---- enum to label mappings ----
    //
    // The side / type / status mappings live inline inside `render_positions`
    // and `render_orders` and their results go straight to stdout, so the label
    // text itself is not observable from a unit test. What these tests do pin is
    // that every `buffa::EnumValue::Unknown(_)` value reaches the `_ => "-"`
    // catch-all without panicking, which is the arm that would otherwise stay
    // uncovered for the whole life of the crate.

    #[test]
    fn a_position_with_an_unknown_side_reaches_the_placeholder_branch() {
        for wire in [0i32, 7, i32::MAX, i32::MIN] {
            let position =
                utrading::Position { side: buffa::EnumValue::Unknown(wire), ..Default::default() };
            table().render_positions(std::slice::from_ref(&position));
        }
    }

    #[test]
    fn an_order_with_unknown_side_type_and_status_reaches_the_placeholder_branch() {
        for wire in [0i32, 42, i32::MAX, i32::MIN] {
            let order = utrading::Order {
                side: buffa::EnumValue::Unknown(wire),
                r#type: buffa::EnumValue::Unknown(wire),
                status: buffa::EnumValue::Unknown(wire),
                ..Default::default()
            };
            table().render_orders(std::slice::from_ref(&order));
        }
    }

    #[test]
    fn an_order_with_known_side_type_and_status_renders_through_the_named_branches() {
        let order = utrading::Order {
            side: buffa::EnumValue::Known(utrading::OrderSide::Buy),
            r#type: buffa::EnumValue::Known(utrading::OrderType::StopLimit),
            status: buffa::EnumValue::Known(utrading::OrderStatus::Filled),
            ..Default::default()
        };
        table().render_orders(std::slice::from_ref(&order));
    }

    #[test]
    fn a_position_with_a_known_side_renders_through_the_named_branches() {
        for side in [utrading::OrderSide::Buy, utrading::OrderSide::Sell] {
            let position =
                utrading::Position { side: buffa::EnumValue::Known(side), ..Default::default() };
            table().render_positions(std::slice::from_ref(&position));
        }
    }

    // ---- renderer smoke coverage for both output formats ----

    #[test]
    fn every_renderer_accepts_an_empty_payload_in_both_formats() {
        for renderer in [table(), json()] {
            renderer.render_symbols(&[]);
            renderer.render_candles(&[]);
            renderer.render_account(&utrading::Account::default());
            renderer.render_positions(&[]);
            renderer.render_orders(&[]);
            renderer.render_venues(&[]);
            renderer.render_msg("hello");
        }
    }

    #[test]
    fn the_json_format_serialises_a_populated_payload() {
        let order = utrading::Order {
            id: "order-1".to_string(),
            symbol: "BTCUSDT".to_string(),
            amount: dec(1, 0).into(),
            ..Default::default()
        };
        json().render_orders(std::slice::from_ref(&order));
    }
}
