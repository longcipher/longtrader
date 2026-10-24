//! Part of the session control plane; see [`super::SessionManager`].

use std::time::{SystemTime, UNIX_EPOCH};

use super::{HashMap, ManagerError, Ordering, SessionHandle, SessionManager, SessionState};
use crate::proto::{common, trading, worker};

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| {
        // saturate on overflow (far future) rather than silently returning epoch
        i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
    })
}

/// Millis remainder (<1000) scaled to nanos always fits i32 (<1e9).
fn nanos_from_ms_rem(ms: i64) -> i32 {
    let nanos = ms.rem_euclid(1000) * 1_000_000;
    debug_assert!((0..1_000_000_000).contains(&nanos), "ms remainder nanos fits i32");
    i32::try_from(nanos).unwrap_or(0)
}

impl SessionManager {
    /// Record a strategy log event into the ring/broadcast bus.
    pub async fn report_log(
        &self,
        session_id: &str,
        level: worker::LogLevel,
        message: String,
        fields: HashMap<String, String>,
    ) -> Result<(), ManagerError> {
        let handle = self.get(session_id).await?;
        let now = now_ms();
        let event = worker::LogEvent {
            session_id: session_id.to_string(),
            level: buffa::EnumValue::Known(level),
            message: message.clone(),
            timestamp: buffa::MessageField::some(buffa_types::google::protobuf::Timestamp {
                seconds: now.div_euclid(1000),
                nanos: nanos_from_ms_rem(now),
                ..Default::default()
            }),
            fields: fields.into_iter().collect(),
            ..Default::default()
        };
        // The session-scoped counter is the only one: it moves for every accepted
        // event, so logs reported before `RegisterStrategy` are counted once the
        // record appears instead of being reported as zero forever. The record's
        // own field is overwritten on read (`SessionHandle::strategy_info`).
        handle.log_events.fetch_add(1, Ordering::Relaxed);
        match level {
            worker::LogLevel::Error => {
                tracing::error!(session = %session_id, "{message}");
            }
            worker::LogLevel::Warn => {
                tracing::warn!(session = %session_id, "{message}");
            }
            worker::LogLevel::Debug => {
                tracing::debug!(session = %session_id, "{message}");
            }
            _ => {
                tracing::info!(session = %session_id, "{message}");
            }
        }
        handle
            .publish(worker::StrategyEvent {
                header: buffa::MessageField::some(common::EventHeader::default()),
                event: Some(worker::strategy_event::Event::Log(Box::new(event))),
                resume_token: String::new(),
                ..Default::default()
            })
            .await;
        Ok(())
    }

    /// Publish an order update attributed to this session.
    pub async fn publish_order_update(
        &self,
        session_id: &str,
        order: trading::Order,
        update_type: String,
    ) -> Result<(), ManagerError> {
        let handle = self.get(session_id).await?;
        handle
            .publish(worker::StrategyEvent {
                header: buffa::MessageField::some(common::EventHeader::default()),
                event: Some(worker::strategy_event::Event::OrderUpdate(Box::new(
                    worker::OrderUpdate {
                        order: buffa::MessageField::some(order),
                        update_type,
                        ..Default::default()
                    },
                ))),
                resume_token: String::new(),
                ..Default::default()
            })
            .await;
        Ok(())
    }

    pub(crate) async fn publish_state_change(
        &self,
        handle: &SessionHandle,
        from: SessionState,
        to: SessionState,
        reason: &str,
    ) {
        handle
            .publish(worker::StrategyEvent {
                header: buffa::MessageField::some(common::EventHeader::default()),
                event: Some(worker::strategy_event::Event::StateChange(Box::new(
                    worker::StateChange {
                        from: buffa::EnumValue::Known(from.to_proto()),
                        to: buffa::EnumValue::Known(to.to_proto()),
                        reason: reason.to_string(),
                        ..Default::default()
                    },
                ))),
                resume_token: String::new(),
                ..Default::default()
            })
            .await;
    }
}
