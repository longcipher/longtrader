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
use longtrader_proto::client::TerminalClient;

use crate::output::Renderer;

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
) {
    match cmd {
        Commands::Health => health::run(client, output).await,
        Commands::Venues => venues::run(client, output).await,
        Commands::Symbols { venue } => symbols::run(client, &venue, output).await,
        Commands::Candles { symbol, timeframe, limit } => {
            candles::run(client, venue, &symbol, &timeframe, limit, output).await;
        }
        Commands::Book { symbol, depth } => book::run(client, venue, &symbol, depth, output).await,
        Commands::Search { query, limit } => {
            search::run(client, venue, &query, limit, output).await;
        }
        Commands::Account => match client.get_account(venue).await {
            Ok(account) => output.render_account(&account),
            Err(e) => output.render_msg(&format!("Error: {e}")),
        },
        Commands::Positions => positions::run(client, venue, output).await,
        Commands::Orders { symbol } => match client.get_open_orders(venue, symbol.as_deref()).await
        {
            Ok(orders) => output.render_orders(&orders),
            Err(e) => output.render_msg(&format!("Error: {e}")),
        },
        Commands::History { limit } => match client.get_order_history(venue, limit).await {
            Ok(orders) => output.render_orders(&orders),
            Err(e) => output.render_msg(&format!("Error: {e}")),
        },
        Commands::Buy { symbol, quantity, price, take_profit, stop_loss } => {
            orders::place_order(
                client,
                venue,
                &symbol,
                longtrader_proto::proto::longtrader::terminal::v1::Side::Buy,
                &quantity,
                price.as_deref(),
                take_profit.as_deref(),
                stop_loss.as_deref(),
                output,
            )
            .await;
        }
        Commands::Sell { symbol, quantity, price, take_profit, stop_loss } => {
            orders::place_order(
                client,
                venue,
                &symbol,
                longtrader_proto::proto::longtrader::terminal::v1::Side::Sell,
                &quantity,
                price.as_deref(),
                take_profit.as_deref(),
                stop_loss.as_deref(),
                output,
            )
            .await;
        }
        Commands::Cancel { order_id } => match client.cancel_order(venue, &order_id).await {
            Ok(order) => output.render_orders(&[order]),
            Err(e) => output.render_msg(&format!("Error: {e}")),
        },
        Commands::Close { position_id } => match client.close_position(venue, &position_id).await {
            Ok(()) => output.render_msg(&format!("Position {position_id} closed")),
            Err(e) => output.render_msg(&format!("Error: {e}")),
        },
        Commands::Stream { topics } => stream::run(client, venue, &topics, output).await,
        Commands::Strategies => strategies::list(client, output).await,
        Commands::StrategyStart { strategy_id, name } => {
            strategies::start(client, &strategy_id, name.as_deref(), output).await;
        }
        Commands::StrategyStop { strategy_id, cancel_all } => {
            strategies::stop(client, &strategy_id, cancel_all, output).await;
        }
    }
}
