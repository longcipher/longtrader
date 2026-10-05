pub(crate) mod book;
pub(crate) mod candles;
pub(crate) mod health;
pub(crate) mod orders;
pub(crate) mod positions;
pub(crate) mod search;
pub(crate) mod strategies;
pub(crate) mod stream;
pub(crate) mod symbols;
pub(crate) mod venues;

use clap::Subcommand;
use color_eyre::Result;
use longtrader_proto::{
    client::TerminalClient,
    proto::longtrader::{common::v1 as common, trading::v1 as utrading},
};

use crate::output::Renderer;

/// Build the canonical exchange selector from the CLI's `--venue` flag.
///
/// An empty id means "the backend's single active venue", which is what the
/// empty-string venue meant before the contract convergence.
pub(crate) fn exchange_id(venue: &str) -> common::ExchangeId {
    common::ExchangeId { id: venue.to_string(), ..Default::default() }
}

#[derive(Subcommand, Debug)]
pub(crate) enum Commands {
    /// Health check
    Health,
    /// List connected venues
    Venues,
    /// List tradeable symbols
    Symbols {
        #[arg(long, default_value = "mock")]
        venue: String,
    },
    /// Get OHLCV candles
    Candles {
        symbol: String,
        #[arg(long, default_value = "M1")]
        timeframe: String,
        #[arg(long, default_value = "200")]
        limit: u32,
    },
    /// Get order book snapshot
    Book {
        symbol: String,
        #[arg(long, default_value = "10")]
        depth: u32,
    },
    /// Search symbols
    Search {
        query: String,
        #[arg(long, default_value = "50")]
        limit: u32,
    },
    /// Get account balances
    Account,
    /// Get open positions
    Positions,
    /// Get open orders
    Orders {
        #[arg(long)]
        symbol: Option<String>,
    },
    /// Get order history
    History {
        #[arg(long, default_value = "100")]
        limit: u32,
    },
    /// Place a buy order
    Buy {
        symbol: String,
        quantity: String,
        #[arg(long)]
        price: Option<String>,
        #[arg(long)]
        take_profit: Option<String>,
        #[arg(long)]
        stop_loss: Option<String>,
    },
    /// Place a sell order
    Sell {
        symbol: String,
        quantity: String,
        #[arg(long)]
        price: Option<String>,
        #[arg(long)]
        take_profit: Option<String>,
        #[arg(long)]
        stop_loss: Option<String>,
    },
    /// Cancel an order
    Cancel { order_id: String },
    /// Close a position
    Close { position_id: String },
    /// Stream live updates
    Stream {
        #[arg(long, value_delimiter = ',')]
        topics: Vec<String>,
    },
    /// List strategies
    Strategies,
    /// Start a strategy
    StrategyStart {
        strategy_id: String,
        #[arg(long)]
        name: Option<String>,
    },
    /// Stop a strategy
    StrategyStop {
        strategy_id: String,
        #[arg(long)]
        cancel_all: bool,
    },
}

pub(crate) async fn dispatch(
    client: &TerminalClient,
    venue: &str,
    output: &Renderer,
    cmd: Commands,
) -> Result<()> {
    let exchange = exchange_id(venue);
    match cmd {
        Commands::Health => health::run(client, output).await?,
        Commands::Venues => venues::run(client, output).await?,
        Commands::Symbols { venue } => symbols::run(client, &venue, output).await?,
        Commands::Candles { symbol, timeframe, limit } => {
            candles::run(client, &exchange, &symbol, &timeframe, limit, output).await?;
        }
        Commands::Book { symbol, depth } => {
            book::run(client, &exchange, &symbol, depth, output).await?;
        }
        Commands::Search { query, limit } => {
            search::run(client, &exchange, &query, limit, output).await?;
        }
        Commands::Account => {
            let account = client.get_account(&exchange).await?;
            output.render_account(&account);
        }
        Commands::Positions => positions::run(client, &exchange, output).await?,
        Commands::Orders { symbol } => {
            let orders =
                client.fetch_open_orders(&exchange, symbol.as_deref().unwrap_or(""), 100).await?;
            output.render_orders(&orders);
        }
        Commands::History { limit } => {
            let orders = client.get_order_history(&exchange, limit).await?;
            output.render_orders(&orders);
        }
        Commands::Buy { symbol, quantity, price, take_profit, stop_loss } => {
            orders::place_order(
                client,
                &exchange,
                &symbol,
                utrading::OrderSide::Buy,
                &quantity,
                price.as_deref(),
                take_profit.as_deref(),
                stop_loss.as_deref(),
                output,
            )
            .await?;
        }
        Commands::Sell { symbol, quantity, price, take_profit, stop_loss } => {
            orders::place_order(
                client,
                &exchange,
                &symbol,
                utrading::OrderSide::Sell,
                &quantity,
                price.as_deref(),
                take_profit.as_deref(),
                stop_loss.as_deref(),
                output,
            )
            .await?;
        }
        Commands::Cancel { order_id } => {
            let req = utrading::CancelOrderRequest {
                exchange_id: exchange.clone().into(),
                order_id: order_id.clone(),
                ..Default::default()
            };
            let order = client.cancel_order(req).await?;
            output.render_orders(&[order]);
        }
        Commands::Close { position_id } => {
            let pos = client.close_position(&exchange, &position_id).await?;
            output.render_msg(&format!(
                "Position {} closed (unrealized pnl={})",
                pos.id,
                crate::output::fmt_dec(&pos.unrealized_pnl)
            ));
        }
        Commands::Stream { topics } => stream::run(client, &exchange, &topics, output).await?,
        Commands::Strategies => strategies::list(client, output).await?,
        Commands::StrategyStart { strategy_id, name } => {
            strategies::start(client, &strategy_id, name.as_deref(), output).await?;
        }
        Commands::StrategyStop { strategy_id, cancel_all } => {
            strategies::stop(client, &strategy_id, cancel_all, output).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// A contract selector built the way `exchange_id` is expected to build one:
    /// the `id` half filled from the venue, the `label` half left at its default.
    fn selector(id: &str) -> common::ExchangeId {
        common::ExchangeId { id: id.to_string(), ..Default::default() }
    }

    // ---- exchange_id: the empty selector ----

    #[test]
    fn an_empty_venue_selects_the_backends_single_active_venue() {
        // Documented contract: an empty `ExchangeId.id` resolves server-side to
        // whichever venue the terminal has active, which is what the CLI's
        // `--venue ""` means.
        let selector = exchange_id("");
        assert_eq!(selector.id, "");
        assert_eq!(selector.label, "");
    }

    #[test]
    fn the_empty_venue_selector_is_wire_identical_to_the_default_message() {
        // Recorded ambiguity: because nothing on the wire distinguishes an
        // all-defaults `ExchangeId` from a deliberately empty selector, a server
        // cannot tell "the caller chose the active venue" from "the caller forgot
        // to say". Both decode to the same bytes.
        assert_eq!(exchange_id(""), common::ExchangeId::default());
    }

    #[test]
    fn a_venue_label_passes_through_untouched() {
        let selector = exchange_id("binance");
        assert_eq!(selector.id, "binance");
    }

    #[test]
    fn the_label_field_is_never_populated_by_the_venue_flag() {
        // `common.v1.ExchangeId` has both an `id` and a `label`, but the CLI only
        // ever fills `id`, so the label half of every request is empty.
        for venue in ["", "mock", "binance", "BINANCE", " with spaces "] {
            assert_eq!(exchange_id(venue).label, "", "venue={venue:?}");
        }
    }

    #[test]
    fn a_venue_is_never_normalised_trimmed_or_case_folded() {
        // No `to_uppercase`, no `trim`, no defaulting: whatever the shell passed
        // is what the backend is asked for, so a typo is a backend error rather
        // than a silent correction here.
        for venue in ["mock", "MOCK", " mock", "mock ", "Mock"] {
            assert_eq!(exchange_id(venue).id, venue);
        }
    }

    #[test]
    fn the_selector_populates_the_id_half_and_nothing_else() {
        assert_eq!(exchange_id("binance"), selector("binance"));
        assert_eq!(exchange_id("binance").label, "");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// `exchange_id` is the identity on the venue string plus a fixed empty
        /// label: whatever goes in comes back out of `id`, and `label` is never
        /// touched. Nothing about the string can be reinterpreted on the way.
        #[test]
        fn exchange_id_is_the_identity_on_the_venue_string(venue in ".{0,24}") {
            let selector = exchange_id(&venue);
            prop_assert_eq!(selector.id.as_str(), venue.as_str(), "venue={:?}", venue);
            prop_assert_eq!(selector.label, "");
        }

        /// The selector is injective on the venue string: two venues produce the
        /// same request only when they are the same venue, so the CLI can never
        /// silently collapse two callers onto one backend selector.
        #[test]
        fn two_venues_share_a_selector_only_when_they_are_the_same_venue(
            left in ".{0,12}",
            right in ".{0,12}",
        ) {
            prop_assert_eq!(exchange_id(&left) == exchange_id(&right), left == right);
        }
    }
}
