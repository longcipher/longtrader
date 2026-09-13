//! Shared Connect-RPC unary transport used by every client in the workspace.
//!
//! The worker's `RemoteAdapter` and this crate's `TerminalClient` both delegate
//! their unary calls here so the URL construction, bearer auth, HTTP status
//! handling and protobuf decode logic is implemented exactly once (DRY).

use buffa::Message;
use thiserror::Error;

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

/// Perform one Connect unary call: POST `base_url/{service}/{method}` with
/// `application/proto` and an optional bearer token, then decode the response.
pub async fn unary<Q: Message, R: Message + Default>(
    http: &hpx::Client,
    base_url: &str,
    service: &str,
    method: &str,
    token: &str,
    req: Q,
) -> Result<R, TransportError> {
    let url = format!("{}/{}/{}", base_url.trim_end_matches('/'), service, method);
    let body = req.encode_to_vec();
    let mut builder = http.post(&url).header("content-type", "application/proto").body(body);
    if !token.is_empty() {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let resp = builder.send().await.map_err(|e| TransportError::Http(e.to_string()))?;
    let status = resp.status();
    if !status.is_success() {
        let bytes =
            resp.bytes().await.map_err(|e| TransportError::Http(format!("read error: {e}")))?;
        let text = String::from_utf8_lossy(&bytes).to_string();
        return Err(TransportError::Rpc { code: u32::from(status.as_u16()), message: text });
    }
    let bytes = resp.bytes().await.map_err(|e| TransportError::Http(format!("read error: {e}")))?;
    R::decode_from_slice(&bytes)
        .map_err(|e| TransportError::Decode(format!("decode {method}: {e}")))
}
