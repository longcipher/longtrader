use longtrader_proto::client::TerminalClient;

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    venue: &str,
    symbol: &str,
    depth: u32,
    output: &Renderer,
) {
    match client.get_book(venue, symbol, depth).await {
        Ok(book) => {
            if let Renderer { format: crate::output::OutputFormat::Json, .. } = output {
                println!("{}", serde_json::to_string_pretty(&book).unwrap_or_default());
            } else {
                println!("Symbol: {}", book.symbol);
                println!("Timestamp: {}", book.timestamp_ms);
                println!();
                println!("{:<15} {:<15}", "BID PRICE", "BID AMOUNT");
                println!("{}", "-".repeat(30));
                for level in &book.bids {
                    println!("{:<15} {:<15}", level.price, level.amount);
                }
                println!();
                println!("{:<15} {:<15}", "ASK PRICE", "ASK AMOUNT");
                println!("{}", "-".repeat(30));
                for level in &book.asks {
                    println!("{:<15} {:<15}", level.price, level.amount);
                }
            }
        }
        Err(e) => output.render_msg(&format!("Error: {e}")),
    }
}
