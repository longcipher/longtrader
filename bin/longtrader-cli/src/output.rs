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
/// The numeric pair is authoritative when `raw_str` is empty; falling back to
/// the raw string keeps an out-of-range mantissa visible instead of printing a
/// wrong number.
pub(crate) fn fmt_dec(d: &common::Decimal) -> String {
    longtrader_contract::ext::common_to_decimal(d)
        .map_or_else(|_| d.raw_str.clone(), |v| v.to_string())
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
