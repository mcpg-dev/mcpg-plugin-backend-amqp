//! AMQP structured response envelope — the `BackendResponse.payload` the
//! gateway projects onto `tools/call`. A non-null `downstreamError` slot is
//! the gateway's `is_error` signal (same contract as the http/soap/ldap/mssql
//! backends).

use serde_json::{Value, json};

use crate::amqp::AmqpOutcome;

/// Build a downstream-error object for the envelope's `downstreamError` slot.
pub fn amqp_downstream_error(kind: &str, message: &str, retryable: bool) -> Value {
    json!({
        "kind": kind,
        "code": format!("mcpg.downstream_amqp.{kind}"),
        "message": message,
        "retryable": retryable,
        "retryClass": if retryable { "with_backoff" } else { "do_not_retry" },
        "suggestedAction": if retryable { "check_broker_connectivity_and_retry" } else { "inspect_amqp_error" },
    })
}

/// Classify a failure string. Connection-level failures (connect / channel /
/// timeout / dropped connection) are retryable transport errors; broker
/// rejections (nack, unroutable) are caller/config problems and are not.
pub fn classify_error(message: &str) -> Value {
    let lower = message.to_ascii_lowercase();
    let retryable = lower.contains("connect")
        || lower.contains("channel failed")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("stream ended")
        || lower.contains("broken pipe")
        || lower.contains("connection reset");
    let kind = if retryable {
        "transport_error"
    } else {
        "amqp_error"
    };
    amqp_downstream_error(kind, message, retryable)
}

/// Build the AMQP structured-content envelope returned as the
/// `BackendResponse.payload`.
#[allow(clippy::too_many_arguments)]
pub fn build_result_envelope(
    tool_name: &str,
    profile_name: &str,
    op: &str,
    exchange: &str,
    routing_key: &str,
    queue: &str,
    outcome: Option<&AmqpOutcome>,
    duration_ms: u128,
    downstream_error: Option<&Value>,
    error: Option<&str>,
) -> Value {
    let response = match outcome {
        Some(o) => json!({
            "published": o.published,
            "message": o.message,
            "found": o.found,
            "durationMs": duration_ms,
        }),
        None => Value::Null,
    };
    json!({
        "toolName": tool_name,
        "profile": profile_name,
        "request": {
            "op": op,
            "exchange": exchange,
            "routingKey": routing_key,
            "queue": queue,
        },
        "response": response,
        "downstreamError": downstream_error,
        "downstreamErrors": downstream_error
            .map(|d| vec![d.clone()])
            .unwrap_or_default(),
        "error": error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_failure_is_retryable_transport_error() {
        let e = classify_error("AMQP connect failed: connection refused");
        assert_eq!(e["kind"], json!("transport_error"));
        assert_eq!(e["retryable"], json!(true));
    }

    #[test]
    fn nack_is_not_retryable() {
        let e = classify_error("AMQP broker nacked the publish");
        assert_eq!(e["kind"], json!("amqp_error"));
        assert_eq!(e["retryable"], json!(false));
    }

    #[test]
    fn publish_envelope_shape() {
        let outcome = AmqpOutcome {
            published: Some(true),
            message: None,
            found: None,
        };
        let env = build_result_envelope(
            "jobs.enqueue",
            "jobs.enqueue",
            "publish",
            "",
            "jobs",
            "",
            Some(&outcome),
            3,
            None,
            None,
        );
        assert_eq!(env["response"]["published"], json!(true));
        assert!(env["downstreamError"].is_null());
    }

    #[test]
    fn error_envelope_nulls_response() {
        let d = classify_error("AMQP broker nacked the publish");
        let env = build_result_envelope(
            "jobs.enqueue",
            "jobs.enqueue",
            "publish",
            "",
            "jobs",
            "",
            None,
            2,
            Some(&d),
            Some("nacked"),
        );
        assert!(env["response"].is_null());
        assert_eq!(env["downstreamError"]["kind"], json!("amqp_error"));
    }
}
