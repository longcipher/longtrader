use color_eyre::Result;
use longtrader_proto::{client::TerminalClient, proto::longtrader::common::v1 as common};

use crate::output::{Renderer, fmt_dec};

pub(crate) async fn run(
    client: &TerminalClient,
    exchange: &common::ExchangeId,
    symbol: &str,
    depth: u32,
    output: &Renderer,
) -> Result<()> {
    match client.fetch_order_book(exchange, symbol, depth).await {
        Ok(book) => {
            if let Renderer { format: crate::output::OutputFormat::Json, .. } = output {
                println!("{}", serde_json::to_string_pretty(&book).unwrap_or_default());
            } else {
                println!("Symbol: {}", book.symbol);
                if let Some(ts) = book.timestamp.as_option() {
                    println!("Timestamp: {}.{:09}", ts.seconds, ts.nanos);
                }
                println!();
                println!("{:<15} {:<15}", "BID PRICE", "BID AMOUNT");
                println!("{}", "-".repeat(30));
                for level in &book.bids {
                    println!("{:<15} {:<15}", fmt_dec(&level.price), fmt_dec(&level.amount));
                }
                println!();
                println!("{:<15} {:<15}", "ASK PRICE", "ASK AMOUNT");
                println!("{}", "-".repeat(30));
                for level in &book.asks {
                    println!("{:<15} {:<15}", fmt_dec(&level.price), fmt_dec(&level.amount));
                }
            }
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
