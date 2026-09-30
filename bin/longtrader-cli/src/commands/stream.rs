use color_eyre::Result;
use futures_util::StreamExt;
use longtrader_proto::{
    client::TerminalClient,
    proto::longtrader::{common::v1 as common, stream::v1 as stream},
};

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    exchange: &common::ExchangeId,
    topics: &[String],
    output: &Renderer,
) -> Result<()> {
    let topic_classes: Vec<stream::TopicClass> = if topics.is_empty() {
        vec![
            stream::TopicClass::MarketLite,
            stream::TopicClass::MarketHeavy,
            stream::TopicClass::Trading,
        ]
    } else {
        let mut classes = Vec::new();
        let mut unknown = Vec::new();
        for t in topics {
            match t.to_uppercase().as_str() {
                "MARKET_LITE" => classes.push(stream::TopicClass::MarketLite),
                "MARKET_HEAVY" => classes.push(stream::TopicClass::MarketHeavy),
                "TRADING" => classes.push(stream::TopicClass::Trading),
                "RUNTIME" => classes.push(stream::TopicClass::Runtime),
                "FUNDING" => classes.push(stream::TopicClass::Funding),
                "DERIVATIVES" => classes.push(stream::TopicClass::Derivatives),
                other => unknown.push(other.to_string()),
            }
        }
        if !unknown.is_empty() {
            return Err(color_eyre::Report::msg(format!(
                "Unknown topic class(es): {}",
                unknown.join(", ")
            )));
        }
        classes
    };

    let mut stream = client.stream_updates(exchange, &[], &topic_classes).await?;
    output.render_msg("Streaming updates (Ctrl+C to stop)...");
    loop {
        tokio::select! {
            maybe = stream.next() => {
                match maybe {
                    Some(Ok(env)) => {
                        let payload_desc = match &env.payload {
                            Some(p) => describe_payload(p),
                            None => "empty".to_string(),
                        };
                        output.render_msg(&format!(
                            "[{}] seq={} {}",
                            topic_name(env.topic_class),
                            env.seq,
                            payload_desc
                        ));
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                }
            }
            _ = tokio::signal::ctrl_c() => {
                output.render_msg("Stopped by user (Ctrl+C)");
                break;
            }
        }
    }
    Ok(())
}

fn topic_name(topic: buffa::EnumValue<stream::TopicClass>) -> &'static str {
    match topic {
        buffa::EnumValue::Known(stream::TopicClass::MarketLite) => "MARKET_LITE",
        buffa::EnumValue::Known(stream::TopicClass::MarketHeavy) => "MARKET_HEAVY",
        buffa::EnumValue::Known(stream::TopicClass::Trading) => "TRADING",
        buffa::EnumValue::Known(stream::TopicClass::Runtime) => "RUNTIME",
        buffa::EnumValue::Known(stream::TopicClass::Funding) => "FUNDING",
        buffa::EnumValue::Known(stream::TopicClass::Derivatives) => "DERIVATIVES",
        _ => "UNKNOWN",
    }
}

fn describe_payload(payload: &stream::update_envelope::Payload) -> String {
    use stream::update_envelope::Payload;

    use crate::output::fmt_dec;
    match payload {
        Payload::Tick(t) => format!("Tick {} @ {}", t.symbol, fmt_dec(&t.last)),
        Payload::Book(b) => {
            format!("Book {} ({} bids, {} asks)", b.symbol, b.bids.len(), b.asks.len())
        }
        Payload::Trade(t) => {
            format!("Trade {} {} @ {}", t.symbol, fmt_dec(&t.amount), fmt_dec(&t.price))
        }
        Payload::Order(o) => format!("Order {} {}", o.id, o.symbol),
        Payload::Position(p) => format!("Position {} {}", p.id, p.symbol),
        Payload::Account(a) => format!("Account balance={}", fmt_dec(&a.balance)),
        Payload::Ticker(t) => format!("Ticker {} last={}", t.symbol, fmt_dec(&t.last)),
        Payload::FundingRate(f) => {
            format!("FundingRate {} rate={}", f.symbol, fmt_dec(&f.rate))
        }
        Payload::Execution(e) => format!("Execution {} {}", e.order_id, e.symbol),
        Payload::RuntimeStatus(r) => format!("RuntimeStatus connected={}", r.connected),
        _ => "other".to_string(),
    }
}
