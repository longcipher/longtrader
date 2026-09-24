use color_eyre::Result;
use futures_util::StreamExt;
use longtrader_proto::{client::TerminalClient, proto::longtrader::terminal::v1::TopicClass};

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    venue: &str,
    topics: &[String],
    output: &Renderer,
) -> Result<()> {
    let topic_classes: Vec<TopicClass> = if topics.is_empty() {
        vec![TopicClass::MarketLite, TopicClass::MarketHeavy, TopicClass::Trading]
    } else {
        let mut classes = Vec::new();
        let mut unknown = Vec::new();
        for t in topics {
            match t.to_uppercase().as_str() {
                "MARKET_LITE" => classes.push(TopicClass::MarketLite),
                "MARKET_HEAVY" => classes.push(TopicClass::MarketHeavy),
                "TRADING" => classes.push(TopicClass::Trading),
                "RUNTIME" => classes.push(TopicClass::Runtime),
                "FUNDING" => classes.push(TopicClass::Funding),
                "DERIVATIVES" => classes.push(TopicClass::Derivatives),
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

    let venues = if venue.is_empty() { vec![] } else { vec![venue.to_string()] };
    let mut stream = client.stream_updates(&venues, &[], &topic_classes).await?;
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

fn topic_name(topic: buffa::EnumValue<TopicClass>) -> &'static str {
    match topic {
        buffa::EnumValue::Known(TopicClass::MarketLite) => "MARKET_LITE",
        buffa::EnumValue::Known(TopicClass::MarketHeavy) => "MARKET_HEAVY",
        buffa::EnumValue::Known(TopicClass::Trading) => "TRADING",
        buffa::EnumValue::Known(TopicClass::Runtime) => "RUNTIME",
        buffa::EnumValue::Known(TopicClass::Funding) => "FUNDING",
        buffa::EnumValue::Known(TopicClass::Derivatives) => "DERIVATIVES",
        _ => "UNKNOWN",
    }
}

fn describe_payload(
    payload: &longtrader_proto::proto::longtrader::terminal::v1::update_envelope::Payload,
) -> String {
    use longtrader_proto::proto::longtrader::terminal::v1::update_envelope::Payload;
    match payload {
        Payload::Tick(t) => format!("Tick {} @ {}", t.symbol, t.price),
        Payload::Book(b) => {
            format!("Book {} ({} bids, {} asks)", b.symbol, b.bids.len(), b.asks.len())
        }
        Payload::Trade(t) => format!("Trade {} {} @ {}", t.symbol, t.amount, t.price),
        Payload::Order(o) => format!("Order {} {}", o.id, o.symbol),
        Payload::Position(p) => format!("Position {} {}", p.id, p.symbol),
        Payload::Account(a) => format!("Account balance={}", a.balance),
        _ => "other".to_string(),
    }
}
