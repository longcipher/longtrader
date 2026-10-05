#![allow(missing_docs, missing_debug_implementations, clippy::print_stdout, clippy::print_stderr)]

mod commands;
mod output;

use clap::Parser;
use commands::Commands;
use output::OutputFormat;

#[derive(Parser, Debug)]
#[command(
    name = "longtrader",
    version,
    about = "LongTrader CLI — market data, trading, and streaming over Connect-RPC"
)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8810", global = true)]
    endpoint: String,

    #[arg(
        long,
        global = true,
        help = "Terminal API bearer token (also read from $LONGTRADER_TOKEN)"
    )]
    token: Option<String>,

    #[arg(long, default_value = "mock", global = true)]
    venue: String,

    #[arg(long, value_enum, default_value = "table", global = true)]
    format: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let token = cli.token.clone().or_else(|| std::env::var("LONGTRADER_TOKEN").ok());
    let client = if let Some(token) = &token {
        longtrader_proto::client::TerminalClient::new_with_token(&cli.endpoint, token)
    } else {
        longtrader_proto::client::TerminalClient::new(&cli.endpoint)
    };

    let output = output::Renderer::new(cli.format);

    commands::dispatch(&client, &cli.venue, &output, cli.command).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::error::ErrorKind;

    use super::*;

    /// The endpoint `main` falls back to when `--endpoint` is absent.
    const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:8810";

    /// The venue `main` falls back to when `--venue` is absent.
    const DEFAULT_VENUE: &str = "mock";

    // ---- defaults ----
    //
    // The `Cli` derive is exercised here rather than in `commands/mod.rs` because
    // `Cli` is private to this module; a child test module can still read its
    // private fields.

    #[test]
    fn a_bare_subcommand_gets_the_documented_endpoint_venue_and_table_format() {
        let cli = Cli::try_parse_from(["longtrader", "health"])
            .expect("`health` with no flags must parse");
        assert_eq!(cli.endpoint, DEFAULT_ENDPOINT);
        assert_eq!(cli.venue, DEFAULT_VENUE);
        assert!(matches!(cli.format, OutputFormat::Table));
        assert!(cli.token.is_none());
    }

    #[test]
    fn a_global_flag_before_the_subcommand_wins_over_its_default() {
        let cli = Cli::try_parse_from(["longtrader", "--endpoint", "http://host:9999", "health"])
            .expect("a leading global flag must parse");
        assert_eq!(cli.endpoint, "http://host:9999");
    }

    #[test]
    fn a_global_flag_after_the_subcommand_wins_over_its_default() {
        // `global = true` is what makes the flag legal in both positions.
        let cli = Cli::try_parse_from(["longtrader", "health", "--endpoint", "http://host:9999"])
            .expect("a trailing global flag must parse");
        assert_eq!(cli.endpoint, "http://host:9999");
    }

    #[test]
    fn a_global_flag_reaches_the_same_field_from_either_position() {
        let before =
            Cli::try_parse_from(["longtrader", "--venue", "binance", "health"]).expect("parses");
        let after =
            Cli::try_parse_from(["longtrader", "health", "--venue", "binance"]).expect("parses");
        assert_eq!(before.venue, "binance");
        assert_eq!(after.venue, "binance");
    }

    #[test]
    fn a_global_flag_repeated_before_the_subcommand_conflicts_with_itself() {
        // `ArgAction::Set` refuses a second command-line occurrence unless the
        // command sets `args_override_self`, which the derive does not. So a shell
        // wrapper that always appends `--venue` cannot also let the user pass one.
        let error = Cli::try_parse_from(["longtrader", "--venue", "a", "--venue", "b", "health"])
            .expect_err("a repeated `Set` flag conflicts with its earlier occurrence");
        assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn a_global_flag_repeated_across_the_subcommand_boundary_does_not_conflict() {
        // The subcommand's copy of a propagated global is parsed into the
        // subcommand's own matcher, where the root's earlier value is not visible,
        // so the self-conflict above does not fire. `propagate_globals` then keeps
        // whichever occurrence has the higher value source and, on a tie, the one
        // from the deeper matcher — i.e. the later one on the command line.
        let cli =
            Cli::try_parse_from(["longtrader", "--venue", "first", "health", "--venue", "second"])
                .expect("a global split across the subcommand boundary must parse");
        assert_eq!(cli.venue, "second");
    }

    #[test]
    fn the_token_flag_is_read_as_an_explicit_option_and_defaults_to_absent() {
        let bare = Cli::try_parse_from(["longtrader", "health"]).expect("parses");
        assert!(bare.token.is_none());

        let with_token =
            Cli::try_parse_from(["longtrader", "health", "--token", "secret"]).expect("parses");
        assert_eq!(with_token.token.as_deref(), Some("secret"));
    }

    #[test]
    fn an_empty_string_is_a_legal_venue_so_the_active_venue_selector_is_reachable() {
        // The "backend's single active venue" selector is `--venue ""`; clap must
        // not treat the empty value as missing.
        let cli = Cli::try_parse_from(["longtrader", "--venue", "", "health"])
            .expect("an empty flag value must parse");
        assert_eq!(cli.venue, "");
    }

    // ---- the shadowed --venue ----
    // `Cli::venue` is `global = true` and `Commands::Symbols` carries its own
    // `--venue`, so the two share an argument id. clap's global propagation fills
    // the subcommand's field when the user did not pass it there, which means
    // there is no observable shadowing: whichever spelling the user uses, the
    // venue reaches `symbols::run`. Pinned because a clap upgrade that stopped
    // propagating here would silently make `symbols` ignore `--venue`.

    #[test]
    fn a_global_venue_reaches_the_symbols_subcommand() {
        let leading =
            Cli::try_parse_from(["longtrader", "--venue", "binance", "symbols"]).expect("parses");
        assert_eq!(leading.venue, "binance", "the global flag still binds");
        let Commands::Symbols { venue } = leading.command else {
            unreachable!("`symbols` parses into the Symbols variant");
        };
        assert_eq!(venue, "binance", "the global value propagates into the subcommand");

        let trailing =
            Cli::try_parse_from(["longtrader", "symbols", "--venue", "binance"]).expect("parses");
        assert_eq!(trailing.venue, "binance", "the subcommand's value also reaches the global");
        let Commands::Symbols { venue } = trailing.command else {
            unreachable!("`symbols` parses into the Symbols variant");
        };
        assert_eq!(venue, "binance", "the subcommand's own flag binds");
    }

    /// Whichever spelling is used, the two `venue` fields agree — `dispatch`
    /// forwards the `Symbols` arm's own copy, so a divergence would silently
    /// route `symbols` at a different venue than every other subcommand.
    #[test]
    fn the_two_venue_fields_never_disagree() {
        for args in [
            ["longtrader", "--venue", "binance", "symbols"].as_slice(),
            ["longtrader", "symbols", "--venue", "binance"].as_slice(),
            ["longtrader", "symbols"].as_slice(),
        ] {
            let cli = Cli::try_parse_from(args).expect("parses");
            let Commands::Symbols { venue } = cli.command else {
                unreachable!("`symbols` parses into the Symbols variant");
            };
            assert_eq!(cli.venue, venue, "the two venue fields diverged for {args:?}");
        }
    }

    #[test]
    fn no_other_subcommand_shadows_the_global_venue_flag() {
        // Every other subcommand has no `venue` of its own, so the global is
        // propagated into it and both spellings land on `Cli::venue`.
        for subcommand in ["health", "venues", "positions", "account", "strategies"] {
            let args = ["longtrader", subcommand, "--venue", "binance"];
            let trailing = match Cli::try_parse_from(args) {
                Ok(cli) => cli,
                Err(error) => {
                    unreachable!("`{subcommand} --venue` must parse after the subcommand: {error}")
                }
            };
            assert_eq!(trailing.venue, "binance", "subcommand {subcommand}");
        }
    }

    // ---- --format ----

    #[test]
    fn the_json_format_flag_selects_the_json_renderer() {
        let cli = Cli::try_parse_from(["longtrader", "health", "--format", "json"])
            .expect("`--format json` must parse");
        assert!(matches!(cli.format, OutputFormat::Json));
    }

    #[test]
    fn the_format_flag_also_parses_before_the_subcommand() {
        let cli = Cli::try_parse_from(["longtrader", "--format", "json", "health"])
            .expect("a leading `--format` must parse");
        assert!(matches!(cli.format, OutputFormat::Json));
    }

    #[test]
    fn an_unknown_format_is_rejected_rather_than_defaulting() {
        let error = Cli::try_parse_from(["longtrader", "health", "--format", "yaml"])
            .expect_err("`yaml` is not one of the two variants");
        assert_eq!(error.kind(), ErrorKind::InvalidValue);
    }

    // ---- failures ----

    #[test]
    fn an_unknown_subcommand_fails() {
        let error = Cli::try_parse_from(["longtrader", "teleport"])
            .expect_err("`teleport` is not a subcommand");
        assert_eq!(error.kind(), ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn a_missing_subcommand_fails_with_the_help_error() {
        // `Parser` sets `arg_required_else_help`, so a bare invocation answers with
        // the help text rather than the `MissingSubcommand` diagnostic.
        let error = Cli::try_parse_from(["longtrader"]).expect_err("a subcommand is required");
        assert_eq!(error.kind(), ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand);
    }

    #[test]
    fn a_missing_subcommand_after_a_global_flag_fails_with_missing_subcommand() {
        // One user value is present, so `arg_required_else_help` does not fire and
        // the `subcommand_required` check reports the real cause instead.
        let error = Cli::try_parse_from(["longtrader", "--endpoint", "http://host:1"])
            .expect_err("a subcommand is still required");
        assert_eq!(error.kind(), ErrorKind::MissingSubcommand);
    }

    #[test]
    fn a_missing_positional_argument_names_the_argument_it_expected() {
        let error =
            Cli::try_parse_from(["longtrader", "candles"]).expect_err("`candles` needs a symbol");
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn an_unknown_global_flag_fails() {
        let error = Cli::try_parse_from(["longtrader", "health", "--nope"])
            .expect_err("`--nope` is not a flag");
        assert_eq!(error.kind(), ErrorKind::UnknownArgument);
    }

    // ---- the subcommand payloads the parsers hand to `dispatch` ----

    #[test]
    fn the_candles_defaults_are_a_minute_bar_of_two_hundred() {
        let cli = Cli::try_parse_from(["longtrader", "candles", "BTCUSDT"]).expect("parses");
        let Commands::Candles { symbol, timeframe, limit } = cli.command else {
            unreachable!("`candles` parses into the Candles variant");
        };
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(timeframe, "M1");
        assert_eq!(limit, 200);
    }

    #[test]
    fn the_order_defaults_leave_the_symbol_filter_absent() {
        let cli = Cli::try_parse_from(["longtrader", "orders"]).expect("parses");
        let Commands::Orders { symbol } = cli.command else {
            unreachable!("`orders` parses into the Orders variant");
        };
        assert!(symbol.is_none(), "dispatch substitutes the empty filter itself");
    }

    #[test]
    fn the_stream_topics_flag_splits_on_commas() {
        let cli = Cli::try_parse_from(["longtrader", "stream", "--topics", "trading,runtime"])
            .expect("parses");
        let Commands::Stream { topics } = cli.command else {
            unreachable!("`stream` parses into the Stream variant");
        };
        assert_eq!(topics, vec!["trading".to_string(), "runtime".to_string()]);
    }

    #[test]
    fn an_absent_stream_topics_flag_yields_the_empty_list_that_selects_the_defaults() {
        // The empty list is not the same as no `--topics` value at all for the
        // *argument* — clap cannot tell them apart here — but it is what the
        // default bucket selection in `stream::run` keys off.
        let cli = Cli::try_parse_from(["longtrader", "stream"]).expect("parses");
        let Commands::Stream { topics } = cli.command else {
            unreachable!("`stream` parses into the Stream variant");
        };
        assert!(topics.is_empty());
    }

    #[test]
    fn the_bracket_flags_are_independent_optional_options() {
        let cli = Cli::try_parse_from([
            "longtrader",
            "buy",
            "BTCUSDT",
            "1",
            "--price",
            "1500",
            "--stop-loss",
            "1400",
        ])
        .expect("parses");
        let Commands::Buy { symbol, quantity, price, take_profit, stop_loss } = cli.command else {
            unreachable!("`buy` parses into the Buy variant");
        };
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(quantity, "1");
        assert_eq!(price.as_deref(), Some("1500"));
        assert_eq!(take_profit, None, "an unpassed bracket stays absent, not empty");
        assert_eq!(stop_loss.as_deref(), Some("1400"));
    }

    #[test]
    fn a_multi_word_subcommand_name_is_kebab_cased() {
        let cli = Cli::try_parse_from(["longtrader", "strategy-start", "s-1"]).expect("parses");
        assert!(matches!(cli.command, Commands::StrategyStart { .. }));

        let stop = Cli::try_parse_from(["longtrader", "strategy-stop", "s-1", "--cancel-all"])
            .expect("parses");
        let Commands::StrategyStop { strategy_id, cancel_all } = stop.command else {
            unreachable!("`strategy-stop` parses into the StrategyStop variant");
        };
        assert_eq!(strategy_id, "s-1");
        assert!(cancel_all);
    }

    #[test]
    fn a_stop_without_cancel_all_is_false_because_the_flag_is_a_plain_switch() {
        let cli = Cli::try_parse_from(["longtrader", "strategy-stop", "s-1"]).expect("parses");
        let Commands::StrategyStop { cancel_all, .. } = cli.command else {
            unreachable!("`strategy-stop` parses into the StrategyStop variant");
        };
        assert!(!cancel_all);
    }
}
