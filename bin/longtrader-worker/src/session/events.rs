//! Part of the session control plane; see [`super::SessionManager`].

use super::{HashMap, ManagerError, Ordering, SessionHandle, SessionManager, SessionState, time};
use crate::proto::{common, worker};

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
        let now = time::now_ms();
        let event = worker::LogEvent {
            session_id: session_id.to_string(),
            level: buffa::EnumValue::Known(level),
            message: message.clone(),
            timestamp: buffa::MessageField::some(time::timestamp_from_ms(now)),
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

    /// Publish a state change attributed to this session.
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
