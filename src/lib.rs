//! AMQP 0.9.1 (RabbitMQ) backend binding plugin for mcpg.
//!
//! Implements [`AmqpBackendPlugin`] — `BackendPlugin` for `kind: "amqp"`.
//! Publishes messages, runs request/reply (RPC), or pulls one message, over a
//! reused lapin connection. The message body is the tool's arguments JSON (the
//! request payload). Structurally mirrors the soap/ldap/mssql backends;
//! AMQP-specific machinery lives in [`amqp`] + [`envelope`].

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mcpg_plugin_protocol::audit::{AuditEvent, AuditOutcome};
use mcpg_plugin_protocol::types::PluginIdentity;
use mcpg_plugin_protocol::{
    BackendError, BackendHost, BackendPlugin, BackendRequest, BackendResponse, PluginManifest,
    firstparty_manifest,
};
use mcpg_plugin_sdk::HostHandle;
use serde_json::{Value, json};
use tokio::sync::{Mutex, RwLock};
use tracing::debug;

mod amqp;
/// cdylib sync bridge.
pub mod cdylib;
mod envelope;
mod types;

use amqp::{ConnSlot, get_connection};
use envelope::{build_result_envelope, classify_error};
pub use types::{AmqpBackendSpec, AmqpOp};

/// Embedded plugin descriptor.
pub const BINDING_DESCRIPTOR_YAML: &str = include_str!("../plugin.yaml");

// --------------------------------------------------------------------- obs

fn audit_action_for_outcome(label: &str) -> Option<&'static str> {
    match label {
        "timeout" => Some("dev.mcpg.backend.amqp.request_timeout"),
        "transport_error" => Some("dev.mcpg.backend.amqp.request_failed"),
        "amqp_error" => Some("dev.mcpg.backend.amqp.operation_rejected"),
        "invalid_spec" => Some("dev.mcpg.backend.amqp.request_failed"),
        _ => None,
    }
}

fn rfc3339_now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn synthetic_system_identity() -> PluginIdentity {
    PluginIdentity {
        kind: "system".into(),
        trust_level: "verified".into(),
        subject_id: Some("dev.mcpg.backend.amqp".into()),
        auth_provider: None,
        issuer: None,
        roles: vec![],
        groups: vec![],
        scopes: vec![],
        attributes: Default::default(),
    }
}

fn finalize_payload(envelope: Value) -> Result<BackendResponse, BackendError> {
    let payload = serde_json::to_vec(&envelope).map_err(|e| BackendError::Transport {
        message: format!("AMQP plugin envelope serialization failed: {e}"),
    })?;
    Ok(BackendResponse {
        payload,
        truncated: false,
    })
}

// ------------------------------------------------------------------ plugin

/// Per-binding AMQP runtime — connection slot + operation. Cheap to clone
/// (the connection slot is shared behind `Arc`).
#[derive(Clone)]
struct MqProfile {
    uri: String,
    op: AmqpOp,
    exchange: String,
    routing_key: String,
    queue: String,
    content_type: String,
    timeout: Duration,
    conn: Arc<ConnSlot>,
}

/// `BackendPlugin` implementation for `kind: "amqp"`.
pub struct AmqpBackendPlugin {
    manifest: PluginManifest,
    profiles: RwLock<BTreeMap<String, MqProfile>>,
    host_handle: OnceLock<HostHandle>,
}

impl Default for AmqpBackendPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl AmqpBackendPlugin {
    #[must_use]
    pub fn new() -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.backend.amqp",
                name: "AMQP Binding",
                class: Backend,
            },
            profiles: RwLock::new(BTreeMap::new()),
            host_handle: OnceLock::new(),
        }
    }

    pub fn set_host_handle(&self, host: HostHandle) -> bool {
        self.host_handle.set(host).is_ok()
    }

    fn host_handle(&self) -> Option<&HostHandle> {
        self.host_handle.get()
    }

    async fn emit_host_observability(
        &self,
        backend_name: &str,
        outcome_label: &'static str,
        reason: Option<&str>,
        identity: Option<&PluginIdentity>,
        request_id: &str,
        duration: Duration,
    ) {
        let Some(host) = self.host_handle() else {
            return;
        };
        host.histogram(
            "mcpg_amqp_backend_latency_seconds",
            duration.as_secs_f64(),
            &[("outcome", outcome_label)],
        );
        host.counter(
            "mcpg_amqp_backend_calls_total",
            1,
            &[("outcome", outcome_label)],
        );
        if let Some(action) = audit_action_for_outcome(outcome_label) {
            let actor = identity.cloned().unwrap_or_else(synthetic_system_identity);
            let mut details = json!({
                "backend": backend_name,
                "duration_ms": duration.as_millis() as u64,
                "outcome": outcome_label,
                "alias": host.alias(),
            });
            if let Some(reason) = reason {
                details
                    .as_object_mut()
                    .expect("json object")
                    .insert("reason".into(), Value::String(reason.to_owned()));
            }
            let event = AuditEvent {
                event_id: format!("amqp-{}-{}", request_id, duration.as_nanos()),
                occurred_at: rfc3339_now(),
                actor,
                action: action.to_owned(),
                resource: Some(format!("amqp-binding://{backend_name}")),
                outcome: AuditOutcome::Failure,
                request_id: Some(request_id.to_owned()),
                node_id: None,
                details,
                prev_event_hash: None,
            };
            let host_for_audit = host.clone();
            if let Err(join_err) = tokio::task::spawn_blocking(move || {
                let _ = host_for_audit.audit_event(event);
            })
            .await
            {
                debug!(target: "mcpg::amqp::host_handle", error = %join_err, "audit spawn_blocking failed");
            }
        }
    }
}

impl std::fmt::Debug for AmqpBackendPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpBackendPlugin")
            .field("id", &self.manifest.id)
            .finish()
    }
}

#[async_trait]
impl BackendPlugin for AmqpBackendPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        "amqp"
    }

    async fn register_profile(
        &self,
        backend_name: &str,
        spec: &Value,
        _host: Arc<dyn BackendHost>,
    ) -> Result<(), BackendError> {
        let parsed: AmqpBackendSpec =
            serde_json::from_value(spec.clone()).map_err(|e| BackendError::InvalidSpec {
                message: format!("AMQP binding spec: {e}"),
            })?;

        let invalid = |m: String| BackendError::InvalidSpec { message: m };
        if !parsed.uri.starts_with("amqp://") && !parsed.uri.starts_with("amqps://") {
            return Err(invalid(format!(
                "uri must start with amqp:// or amqps://, got '{}'",
                parsed.uri
            )));
        }
        // Per-caller cred:// is unsupported (the connection is per-binding,
        // one identity). Operators use the config secret-resolver.
        if parsed.uri.contains("cred://") {
            return Err(invalid(
                "uri must not contain cred:// — per-caller credentials are unsupported; \
                 use ${env.X} / vault:// (resolved at config load)"
                    .into(),
            ));
        }
        if parsed.timeout_ms == 0 {
            return Err(invalid("timeout_ms must be greater than 0".into()));
        }
        match parsed.op {
            AmqpOp::Get if parsed.queue.trim().is_empty() => {
                return Err(invalid("op 'get' requires a non-empty queue".into()));
            }
            AmqpOp::Rpc if parsed.routing_key.trim().is_empty() => {
                return Err(invalid("op 'rpc' requires a non-empty routing_key".into()));
            }
            AmqpOp::Publish
                if parsed.exchange.trim().is_empty() && parsed.routing_key.trim().is_empty() =>
            {
                return Err(invalid(
                    "op 'publish' requires an exchange or a routing_key".into(),
                ));
            }
            _ => {}
        }

        debug!(
            backend = %backend_name,
            op = parsed.op.as_str(),
            exchange = %parsed.exchange,
            routing_key = %parsed.routing_key,
            "registered AMQP binding profile"
        );

        self.profiles.write().await.insert(
            backend_name.to_owned(),
            MqProfile {
                uri: parsed.uri,
                op: parsed.op,
                exchange: parsed.exchange,
                routing_key: parsed.routing_key,
                queue: parsed.queue,
                content_type: parsed.content_type,
                timeout: Duration::from_millis(parsed.timeout_ms),
                conn: Arc::new(Mutex::new(None)),
            },
        );
        Ok(())
    }

    async fn execute(
        &self,
        backend_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        let started = Instant::now();
        let request_id = request.request_id.clone();
        let identity = request.identity.clone();
        let host_span = self.host_handle().map(|h| {
            h.span(
                "amqp_backend.execute",
                json!({ "backend": backend_name, "request_id": request_id }),
            )
        });

        let profile = {
            let guard = self.profiles.read().await;
            match guard.get(backend_name).cloned() {
                Some(p) => p,
                None => {
                    let err = BackendError::ProfileNotFound {
                        backend_name: backend_name.to_owned(),
                    };
                    self.emit_host_observability(
                        backend_name,
                        "profile_not_found",
                        Some(&err.to_string()),
                        identity.as_ref(),
                        &request_id,
                        started.elapsed(),
                    )
                    .await;
                    drop(host_span);
                    return Err(err);
                }
            }
        };

        let tool_name = request
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("mcpg-tool-name"))
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| backend_name.to_owned());

        // The message body is the arguments JSON (empty payload → `{}`).
        let payload: Vec<u8> = if request.payload.is_empty() {
            b"{}".to_vec()
        } else {
            request.payload.clone()
        };

        // Connect + dispatch, bounded by the per-call timeout.
        let work = async {
            let conn = get_connection(&profile.conn, &profile.uri).await?;
            match profile.op {
                AmqpOp::Publish => {
                    amqp::publish(
                        &conn,
                        &profile.exchange,
                        &profile.routing_key,
                        &profile.content_type,
                        &payload,
                    )
                    .await
                }
                AmqpOp::Rpc => {
                    amqp::rpc(
                        &conn,
                        &profile.exchange,
                        &profile.routing_key,
                        &profile.content_type,
                        &request_id,
                        &payload,
                        profile.timeout,
                    )
                    .await
                }
                AmqpOp::Get => amqp::get(&conn, &profile.queue).await,
            }
        };
        let result = match tokio::time::timeout(profile.timeout, work).await {
            Ok(r) => r,
            Err(_) => Err("AMQP operation timed out".to_owned()),
        };

        let (envelope, outcome_label, audit_reason): (Value, &'static str, Option<String>) =
            match result {
                Ok(outcome) => (
                    build_result_envelope(
                        &tool_name,
                        backend_name,
                        profile.op.as_str(),
                        &profile.exchange,
                        &profile.routing_key,
                        &profile.queue,
                        Some(&outcome),
                        started.elapsed().as_millis(),
                        None,
                        None,
                    ),
                    "ok",
                    None,
                ),
                Err(message) => {
                    let downstream = classify_error(&message);
                    let lower = message.to_ascii_lowercase();
                    let label = if lower.contains("timed out") || lower.contains("timeout") {
                        "timeout"
                    } else if downstream["kind"] == json!("transport_error") {
                        "transport_error"
                    } else {
                        "amqp_error"
                    };
                    let env = build_result_envelope(
                        &tool_name,
                        backend_name,
                        profile.op.as_str(),
                        &profile.exchange,
                        &profile.routing_key,
                        &profile.queue,
                        None,
                        started.elapsed().as_millis(),
                        Some(&downstream),
                        Some(&message),
                    );
                    (env, label, Some(message))
                }
            };

        self.emit_host_observability(
            backend_name,
            outcome_label,
            audit_reason.as_deref(),
            identity.as_ref(),
            &request_id,
            started.elapsed(),
        )
        .await;
        drop(host_span);
        finalize_payload(envelope)
    }

    fn audit_metadata(&self, _backend_name: &str) -> serde_json::Map<String, Value> {
        let mut map = serde_json::Map::new();
        map.insert("amqp.transport".to_owned(), json!("plugin"));
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_op_host() -> Arc<dyn BackendHost> {
        Arc::new(NoOpHost)
    }

    fn minimal_spec() -> Value {
        json!({
            "uri": "amqp://guest:guest@localhost:5672/%2f",
            "routing_key": "jobs",
        })
    }

    #[test]
    fn kind_is_amqp() {
        assert_eq!(AmqpBackendPlugin::new().kind(), "amqp");
    }

    #[test]
    fn manifest_id() {
        assert_eq!(
            AmqpBackendPlugin::new().manifest().id,
            "dev.mcpg.backend.amqp"
        );
    }

    #[tokio::test]
    async fn register_accepts_minimal_spec() {
        let plugin = AmqpBackendPlugin::new();
        plugin
            .register_profile("jobs", &minimal_spec(), no_op_host())
            .await
            .expect("register");
        let profiles = plugin.profiles.read().await;
        let p = profiles.get("jobs").unwrap();
        assert_eq!(p.op, AmqpOp::Publish);
        assert_eq!(p.routing_key, "jobs");
    }

    // Conformance: omitting each defaulted field through the actual
    // `register_profile` path materializes the SAME value the gateway's typed
    // `AmqpBackendConfig` (now deleted in Stage 3) produced. This is the
    // single-source-of-truth gate — the plugin owns the defaults.
    #[tokio::test]
    async fn register_materializes_gateway_defaults() {
        let plugin = AmqpBackendPlugin::new();
        // Omit op / exchange / content_type / timeout_ms; supply only the
        // required uri + a routing_key (so publish has a destination).
        let spec = json!({
            "uri": "amqp://guest:guest@localhost:5672/%2f",
            "routing_key": "jobs",
        });
        plugin
            .register_profile("defaults", &spec, no_op_host())
            .await
            .expect("register");
        let profiles = plugin.profiles.read().await;
        let p = profiles.get("defaults").unwrap();
        // op → "publish" (gateway: op.unwrap_or("publish")).
        assert_eq!(p.op, AmqpOp::Publish);
        // exchange → "" (gateway default direct exchange).
        assert_eq!(p.exchange, "");
        // queue → "" (gateway default).
        assert_eq!(p.queue, "");
        // content_type → "application/json" (gateway default_amqp_content_type).
        assert_eq!(p.content_type, "application/json");
        // timeout_ms → 10_000 (gateway default_amqp_timeout_ms).
        assert_eq!(p.timeout, Duration::from_millis(10_000));
    }

    // Conformance: a bad field value ⇒ InvalidSpec (not a silent default).
    #[tokio::test]
    async fn register_rejects_zero_timeout() {
        let plugin = AmqpBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["timeout_ms"] = json!(0);
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("zero timeout");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    // Conformance: an out-of-enum op ⇒ InvalidSpec (deserialize-level reject,
    // mirrors the gateway's `op must be publish|rpc|get`).
    #[tokio::test]
    async fn register_rejects_unknown_op() {
        let plugin = AmqpBackendPlugin::new();
        let spec = json!({ "uri": "amqp://h", "op": "consume", "routing_key": "jobs" });
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("unknown op");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn register_rejects_non_amqp_uri() {
        let plugin = AmqpBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["uri"] = json!("https://broker/");
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("non-amqp");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn register_rejects_cred_uri() {
        let plugin = AmqpBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["uri"] = json!("amqp://cred://vault/amqp@host");
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("cred uri");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn register_rejects_get_without_queue() {
        let plugin = AmqpBackendPlugin::new();
        let spec = json!({ "uri": "amqp://h", "op": "get" });
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("get without queue");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn execute_unknown_profile_is_profile_not_found() {
        let plugin = AmqpBackendPlugin::new();
        let req = BackendRequest {
            payload: vec![],
            headers: vec![],
            request_id: "rq-1".into(),
            session_id: None,
            identity: None,
            idempotency: None,
        };
        let err = plugin.execute("missing", req).await.expect_err("missing");
        assert!(matches!(err, BackendError::ProfileNotFound { .. }));
    }

    struct NoOpHost;

    #[async_trait]
    impl BackendHost for NoOpHost {
        async fn invoke_tool(
            &self,
            _ctx: &mcpg_plugin_protocol::BackendInvocationContext,
            _tool_name: &str,
            _args: &serde_json::Value,
        ) -> Result<serde_json::Value, mcpg_plugin_protocol::BackendHostError> {
            Err(mcpg_plugin_protocol::BackendHostError::NotImplemented)
        }
    }
}
