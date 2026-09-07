use futures_util::StreamExt;
use longtrader_proto::{client::TerminalClient, proto::longtrader::terminal::v1::TopicClass};

use crate::output::Renderer;

pub(crate) async fn run(
    client: &TerminalClient,
    _venue: &str,
    topics: &[String],
    output: &Renderer,
) {
    let topic_classes: Vec<TopicClass> = if topics.is_empty() {
        vec![TopicClass::MarketLite, TopicClass::MarketHeavy, TopicClass::Trading]
    } else {
        topics
            .iter()
            .filter_map(|t| match t.to_uppercase().as_str() {
                "MARKET_LITE" => Some(TopicClass::MarketLite),
                "MARKET_HEAVY" => Some(TopicClass::MarketHeavy),
                "TRADING" => Some(TopicClass::Trading),
                "RUNTIME" => Some(TopicClass::Runtime),
                "FUNDING" => Some(TopicClass::Funding),
                "DERIVATIVES" => Some(TopicClass::Derivatives),
                _ => {
                    output.render_msg(&format!("Unknown topic class: {t}"));
                    None
                }
            })
            .collect()
    };

    match client.stream_updates(&[], &[], &topic_classes).await {
        Ok(mut stream) => {
            output.render_msg("Streaming updates (Ctrl+C to stop)...");
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(env) => {
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
                    Err(e) => {
                        output.render_msg(&format!("Stream error: {e}"));
                        break;
                    }
                }
            }
        }
        Err(e) => output.render_msg(&format!("Error: {e}")),
    }
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
