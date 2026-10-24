//! Shared Connect-RPC unary transport used by every client in the workspace.
//!
//! The worker's `RemoteAdapter` and this crate's `TerminalClient` both delegate
//! their unary calls here so the URL construction, bearer auth, HTTP status
//! handling and protobuf decode logic is implemented exactly once (DRY).

use buffa::Message;
use thiserror::Error;

use crate::client_core::service_url;

/// Transport-level error returned by [`unary`].
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("HTTP transport error: {0}")]
    Http(String),
    #[error("ConnectRPC error ({code}): {message}")]
    Rpc { code: u32, message: String },
    #[error("protobuf decode error: {0}")]
    Decode(String),
}

/// Connect content-type for unary calls (`application/proto`).
/// Server-streaming uses `application/connect+proto` with 5-byte envelopes
/// (see `TerminalClient::stream_updates`); the split is per Connect spec.
pub const CONNECT_PROTO_UNARY: &str = "application/proto";
/// Connect content-type for server-streaming (`application/connect+proto`).
pub const CONNECT_PROTO_STREAM: &str = "application/connect+proto";

/// Default deadline for one unary call (mirrors the Go SDK `unaryTimeout = 10s`).
/// Prevents a hung venue from stalling a strategy tick and the L1 watchdog.
pub const UNARY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Perform one Connect unary call: POST `base_url/{service}/{method}` with
/// `application/proto` and an optional bearer token, then decode the response.
///
/// The URL is composed by the crate's shared `service_url` helper, which owns
/// the trailing-slash and `://` invariants — the caller may pass a base exactly
/// as it was configured.
///
/// Every network wait is bounded by [`UNARY_TIMEOUT`]: a hung backend surfaces
/// as `TransportError::Http("deadline exceeded …")` instead of stalling the
/// caller forever.
pub async fn unary<Q: Message, R: Message + Default>(
    http: &hpx::Client,
    base_url: &str,
    service: &str,
    method: &str,
    token: Option<&str>,
    req: Q,
) -> Result<R, TransportError> {
    let url = service_url(base_url, service, method);
    let body = req.encode_to_vec();
    let mut builder = http.post(&url).header("content-type", CONNECT_PROTO_UNARY).body(body);
    if let Some(token) = token.filter(|t| !t.is_empty()) {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let resp = tokio::time::timeout(UNARY_TIMEOUT, builder.send())
        .await
        .map_err(|_| {
            TransportError::Http(format!("deadline exceeded {service}/{method} after 10s"))
        })
        .and_then(|r| r.map_err(|e| TransportError::Http(e.to_string())))?;
    let status = resp.status();
    if !status.is_success() {
        let bytes = tokio::time::timeout(UNARY_TIMEOUT, resp.bytes())
            .await
            .map_err(|_| {
                TransportError::Http(format!("deadline exceeded reading {service}/{method}"))
            })?
            .map_err(|e| TransportError::Http(format!("read error: {e}")))?;
        let text = String::from_utf8_lossy(&bytes).to_string();
        return Err(TransportError::Rpc { code: u32::from(status.as_u16()), message: text });
    }
    let bytes = tokio::time::timeout(UNARY_TIMEOUT, resp.bytes())
        .await
        .map_err(|_| TransportError::Http(format!("deadline exceeded reading {service}/{method}")))?
        .map_err(|e| TransportError::Http(format!("read error: {e}")))?;
    R::decode_from_slice(&bytes)
        .map_err(|e| TransportError::Decode(format!("decode {method}: {e}")))
}
