use clap::ValueEnum;
use tradingcharts_proto::proto::longtrader::terminal::v1::{
    Account, Candle, Order, Position, Symbol, VenueStatus,
};

#[derive(Debug, Clone, ValueEnum)]
pub(crate) enum OutputFormat {
    Table,
    Json,
}

pub(crate) struct Renderer {
    pub(crate) format: OutputFormat,
}

impl Renderer {
    pub(crate) fn new(format: OutputFormat) -> Self {
        Self { format }
    }

    pub(crate) fn render_symbols(&self, symbols: &[Symbol]) {
        match self.format {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(symbols).unwrap_or_default());
            }
            OutputFormat::Table => {
                println!("{:<15} {:<20} {:<10} {:<10}", "NAME", "DISPLAY", "BASE", "QUOTE");
                println!("{}", "-".repeat(60));
                for s in symbols {
                    println!(
                        "{:<15} {:<20} {:<10} {:<10}",
                        s.name, s.display_name, s.base_asset, s.quote_asset
                    );
                }
            }
        }
    }

    pub(crate) fn render_candles(&self, candles: &[Candle]) {
        match self.format {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(candles).unwrap_or_default());
            }
            OutputFormat::Table => {
                println!(
                    "{:<20} {:<12} {:<12} {:<12} {:<12} {:<12}",
                    "TIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"
                );
                println!("{}", "-".repeat(80));
                for c in candles {
                    println!(
                        "{:<20} {:<12} {:<12} {:<12} {:<12} {:<12}",
                        c.timestamp_ms, c.open, c.high, c.low, c.close, c.volume
                    );
                }
            }
        }
    }

    pub(crate) fn render_account(&self, account: &Account) {
        match self.format {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(account).unwrap_or_default());
            }
            OutputFormat::Table => {
                println!("{:<15} {}", "Balance", account.balance);
                println!("{:<15} {}", "Equity", account.equity);
                println!("{:<15} {}", "Margin Used", account.margin_used);
                println!("{:<15} {}", "Free Margin", account.free_margin);
                println!("{:<15} {}", "Margin Frozen", account.margin_frozen);
            }
        }
    }

    pub(crate) fn render_positions(&self, positions: &[Position]) {
        match self.format {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(positions).unwrap_or_default());
            }
            OutputFormat::Table => {
                println!(
                    "{:<12} {:<12} {:<8} {:<12} {:<12} {:<12} {:<12}",
                    "ID", "SYMBOL", "SIDE", "QTY", "ENTRY", "CURRENT", "PnL"
                );
                println!("{}", "-".repeat(80));
                for p in positions {
                    let side = match p.side {
                        buffa::EnumValue::Known(tradingcharts_proto::proto::longtrader::terminal::v1::PositionSide::Long) => "Long",
                        buffa::EnumValue::Known(tradingcharts_proto::proto::longtrader::terminal::v1::PositionSide::Short) => "Short",
                        _ => "-",
                    };
                    println!(
                        "{:<12} {:<12} {:<8} {:<12} {:<12} {:<12} {:<12}",
                        p.id,
                        p.symbol,
                        side,
                        p.quantity,
                        p.entry_price,
                        p.current_price,
                        p.unrealized_pnl
                    );
                }
            }
        }
    }

    pub(crate) fn render_orders(&self, orders: &[Order]) {
        match self.format {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(orders).unwrap_or_default());
            }
            OutputFormat::Table => {
                println!(
                    "{:<12} {:<12} {:<6} {:<8} {:<10} {:<10} {:<10}",
                    "ID", "SYMBOL", "SIDE", "TYPE", "QTY", "PRICE", "STATUS"
                );
                println!("{}", "-".repeat(70));
                for o in orders {
                    let side = match o.side {
                        buffa::EnumValue::Known(
                            tradingcharts_proto::proto::longtrader::terminal::v1::Side::Buy,
                        ) => "Buy",
                        buffa::EnumValue::Known(
                            tradingcharts_proto::proto::longtrader::terminal::v1::Side::Sell,
                        ) => "Sell",
                        _ => "-",
                    };
                    let otype = match o.order_type {
                        buffa::EnumValue::Known(
                            tradingcharts_proto::proto::longtrader::terminal::v1::OrderType::Market,
                        ) => "Market",
                        buffa::EnumValue::Known(
                            tradingcharts_proto::proto::longtrader::terminal::v1::OrderType::Limit,
                        ) => "Limit",
                        buffa::EnumValue::Known(
                            tradingcharts_proto::proto::longtrader::terminal::v1::OrderType::Stop,
                        ) => "Stop",
                        _ => "-",
                    };
                    let status = match o.status {
                        buffa::EnumValue::Known(tradingcharts_proto::proto::longtrader::terminal::v1::OrderStatus::Pending) => "Pending",
                        buffa::EnumValue::Known(tradingcharts_proto::proto::longtrader::terminal::v1::OrderStatus::Filled) => "Filled",
                        buffa::EnumValue::Known(tradingcharts_proto::proto::longtrader::terminal::v1::OrderStatus::Canceled) => "Canceled",
                        _ => "-",
                    };
                    println!(
                        "{:<12} {:<12} {:<6} {:<8} {:<10} {:<10} {:<10}",
                        o.id,
                        o.symbol,
                        side,
                        otype,
                        o.quantity,
                        o.price.as_deref().unwrap_or("-"),
                        status
                    );
                }
            }
        }
    }

    pub(crate) fn render_venues(&self, venues: &[VenueStatus]) {
        match self.format {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(venues).unwrap_or_default());
            }
            OutputFormat::Table => {
                println!("{:<15} {:<10} {:<10}", "NAME", "CONNECTED", "SYMBOLS");
                println!("{}", "-".repeat(35));
                for v in venues {
                    println!("{:<15} {:<10} {:<10}", v.name, v.connected, v.symbol_count);
                }
            }
        }
    }

    pub(crate) fn render_msg(&self, msg: &str) {
        let _ = self;
        println!("{msg}");
    }
}
