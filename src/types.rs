//! Operator-facing spec for the AMQP backend plugin.
//!
//! One binding = one messaging operation = one MCP tool (or resource). The
//! connection (uri) and the operation (op / exchange / routing_key / queue)
//! live on the per-binding spec, mirroring the http/soap/ldap/mssql
//! one-profile-per-binding shape. The message body is the tool's arguments
//! JSON (the request payload) — reshape upstream with a pipeline transform if
//! a different shape is needed.

use serde::Deserialize;

/// The messaging operation a binding performs.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AmqpOp {
    /// Publish a message (fire-and-forget, with a publisher confirm +
    /// mandatory routing check). Returns whether it was accepted/routed.
    #[default]
    Publish,
    /// Request/reply (RPC): publish with a `reply_to` + `correlation_id`,
    /// then await the correlated reply on a private exclusive queue. Returns
    /// the reply body.
    Rpc,
    /// Pull one message from a queue (`basic_get`, auto-acked). Returns the
    /// message, or null when the queue is empty.
    Get,
}

impl AmqpOp {
    pub fn as_str(self) -> &'static str {
        match self {
            AmqpOp::Publish => "publish",
            AmqpOp::Rpc => "rpc",
            AmqpOp::Get => "get",
        }
    }
}

/// Operator-facing spec the gateway serializes when calling
/// `register_profile`. Mirrors `AmqpBackendConfig` in the gateway crate.
// NOTE: intentionally NOT #[serde(deny_unknown_fields)] — the gateway injects
// the reserved `__mcpg_secret_refs` hint key into this spec at register_profile
// (secret-rotation scoping); denying unknown fields would reject it. The
// operator-facing schema is closed on the gateway-side *BackendConfig instead.
#[derive(Debug, Clone, Deserialize)]
pub struct AmqpBackendSpec {
    /// `amqp://user:pass@host:5672/vhost` or `amqps://…`. Operator-configured
    /// (not caller-templated); the password resolves through the gateway
    /// secret-resolver (`${env.X}` / `vault://…`) at config load.
    pub uri: String,

    /// The operation (default `publish`).
    #[serde(default)]
    pub op: AmqpOp,

    /// Target exchange. Empty = the default (direct) exchange, where
    /// `routing_key` is the destination queue name.
    #[serde(default)]
    pub exchange: String,

    /// Routing key. For `publish`/`rpc` — the binding/queue the message
    /// targets. Operator-fixed (not caller-templated).
    #[serde(default)]
    pub routing_key: String,

    /// Source queue for `get`.
    #[serde(default)]
    pub queue: String,

    /// `content_type` stamped on published messages (default
    /// `application/json` — the arguments are sent as JSON).
    #[serde(default = "default_content_type")]
    pub content_type: String,

    /// Per-call timeout (ms) for connect + publish + (RPC) reply (default
    /// 10 s).
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_content_type() -> String {
    "application/json".into()
}
fn default_timeout_ms() -> u64 {
    10_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_defaults_to_publish() {
        assert_eq!(AmqpOp::default(), AmqpOp::Publish);
    }

    #[test]
    fn spec_applies_defaults() {
        let spec: AmqpBackendSpec = serde_json::from_value(serde_json::json!({
            "uri": "amqp://guest:guest@localhost:5672/%2f",
            "routing_key": "jobs",
        }))
        .unwrap();
        assert_eq!(spec.op, AmqpOp::Publish);
        assert_eq!(spec.exchange, "");
        assert_eq!(spec.routing_key, "jobs");
        assert_eq!(spec.content_type, "application/json");
        assert_eq!(spec.timeout_ms, 10_000);
    }

    #[test]
    fn parses_rpc_and_get() {
        let rpc: AmqpBackendSpec = serde_json::from_value(serde_json::json!({
            "uri": "amqp://h", "op": "rpc", "routing_key": "rpc.add",
        }))
        .unwrap();
        assert_eq!(rpc.op, AmqpOp::Rpc);
        let get: AmqpBackendSpec = serde_json::from_value(serde_json::json!({
            "uri": "amqp://h", "op": "get", "queue": "inbox",
        }))
        .unwrap();
        assert_eq!(get.op, AmqpOp::Get);
        assert_eq!(get.queue, "inbox");
    }
}
