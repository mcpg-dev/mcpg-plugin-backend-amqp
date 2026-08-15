//! AMQP machinery: a reused lapin connection, the three operations
//! (publish / rpc / get), and message → JSON projection.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use futures_util::StreamExt;
use lapin::options::{
    BasicConsumeOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions,
    QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Confirmation, Connection, ConnectionProperties};
use serde_json::{Value, json};
use tokio::sync::Mutex;

/// Lazily-established, reused connection slot for one binding.
pub type ConnSlot = Mutex<Option<Arc<Connection>>>;

/// Get the binding's connection, (re)connecting if absent or dropped.
pub async fn get_connection(slot: &ConnSlot, uri: &str) -> Result<Arc<Connection>, String> {
    let mut guard = slot.lock().await;
    if let Some(conn) = guard.as_ref()
        && conn.status().connected()
    {
        return Ok(conn.clone());
    }
    let conn = Connection::connect(uri, ConnectionProperties::default())
        .await
        .map_err(|e| {
            mcpg_plugin_protocol::redact::redact_in_text(&format!("AMQP connect failed: {e}"))
        })?;
    let conn = Arc::new(conn);
    *guard = Some(conn.clone());
    Ok(conn)
}

/// Outcome of an AMQP operation, projected to the response envelope.
pub struct AmqpOutcome {
    /// `publish`: whether the broker accepted + routed the message.
    pub published: Option<bool>,
    /// `rpc`: the decoded reply body; `get`: the decoded message (or null).
    pub message: Option<Value>,
    /// `get` on an empty queue → `false`.
    pub found: Option<bool>,
}

/// Publish one message (mandatory: an unroutable message is reported, not
/// silently dropped). Returns whether it was accepted and routed.
pub async fn publish(
    conn: &Connection,
    exchange: &str,
    routing_key: &str,
    content_type: &str,
    payload: &[u8],
) -> Result<AmqpOutcome, String> {
    let channel = conn
        .create_channel()
        .await
        .map_err(|e| format!("AMQP channel failed: {e}"))?;
    // Enable publisher confirms so the broker's ack/nack — and the
    // Basic.Return for a `mandatory` unroutable message — actually reach us;
    // without this the publish reports success even when nothing was routed.
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .map_err(|e| format!("AMQP confirm_select failed: {e}"))?;
    let confirm = channel
        .basic_publish(
            exchange.into(),
            routing_key.into(),
            BasicPublishOptions {
                mandatory: true,
                ..BasicPublishOptions::default()
            },
            payload,
            BasicProperties::default().with_content_type(content_type.into()),
        )
        .await
        .map_err(|e| format!("AMQP publish failed: {e}"))?
        .await
        .map_err(|e| format!("AMQP publish confirm failed: {e}"))?;
    let _ = channel.close(200, "done".into()).await;
    match confirm {
        Confirmation::Nack(_) => Err("AMQP broker nacked the publish".to_owned()),
        Confirmation::Ack(Some(_returned)) => {
            Err("AMQP message was unroutable (no queue bound to the routing key)".to_owned())
        }
        Confirmation::Ack(None) | Confirmation::NotRequested => Ok(AmqpOutcome {
            published: Some(true),
            message: None,
            found: None,
        }),
    }
}

/// Request/reply: publish with `reply_to` + `correlation_id`, then await the
/// correlated reply on a private exclusive queue, bounded by `timeout`.
pub async fn rpc(
    conn: &Connection,
    exchange: &str,
    routing_key: &str,
    content_type: &str,
    correlation_id: &str,
    payload: &[u8],
    timeout: Duration,
) -> Result<AmqpOutcome, String> {
    let channel = conn
        .create_channel()
        .await
        .map_err(|e| format!("AMQP channel failed: {e}"))?;
    // Private, server-named, exclusive auto-delete reply queue.
    let reply_queue = channel
        .queue_declare(
            "".into(),
            QueueDeclareOptions {
                exclusive: true,
                auto_delete: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| format!("AMQP reply-queue declare failed: {e}"))?;
    let reply_name = reply_queue.name().to_string();

    let mut consumer = channel
        .basic_consume(
            reply_name.as_str().into(),
            "".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| format!("AMQP reply consume failed: {e}"))?;

    channel
        .basic_publish(
            exchange.into(),
            routing_key.into(),
            BasicPublishOptions {
                mandatory: true,
                ..BasicPublishOptions::default()
            },
            payload,
            BasicProperties::default()
                .with_content_type(content_type.into())
                .with_reply_to(reply_name.clone().into())
                .with_correlation_id(correlation_id.into()),
        )
        .await
        .map_err(|e| format!("AMQP rpc publish failed: {e}"))?
        .await
        .map_err(|e| format!("AMQP rpc publish confirm failed: {e}"))?;

    // Await the reply whose correlation_id matches ours (skip strays).
    loop {
        match tokio::time::timeout(timeout, consumer.next()).await {
            Ok(Some(Ok(delivery))) => {
                let matches = delivery
                    .properties
                    .correlation_id()
                    .as_ref()
                    .map(|c| c.as_str() == correlation_id)
                    .unwrap_or(false);
                if matches {
                    let body = decode_body(&delivery.data);
                    let _ = channel.close(200, "done".into()).await;
                    return Ok(AmqpOutcome {
                        published: None,
                        message: Some(body),
                        found: None,
                    });
                }
                // Not ours — keep waiting.
            }
            Ok(Some(Err(e))) => return Err(format!("AMQP rpc reply stream error: {e}")),
            Ok(None) => return Err("AMQP rpc reply stream ended".to_owned()),
            Err(_) => return Err("AMQP rpc reply timed out".to_owned()),
        }
    }
}

/// Pull one message from `queue` (auto-acked). `found: false` on empty.
pub async fn get(conn: &Connection, queue: &str) -> Result<AmqpOutcome, String> {
    let channel = conn
        .create_channel()
        .await
        .map_err(|e| format!("AMQP channel failed: {e}"))?;
    let got = channel
        .basic_get(queue.into(), BasicGetOptions { no_ack: true })
        .await
        .map_err(|e| format!("AMQP get failed: {e}"))?;
    let _ = channel.close(200, "done".into()).await;
    match got {
        Some(msg) => Ok(AmqpOutcome {
            published: None,
            message: Some(message_to_json(&msg.delivery)),
            found: Some(true),
        }),
        None => Ok(AmqpOutcome {
            published: None,
            message: Some(Value::Null),
            found: Some(false),
        }),
    }
}

/// Project a delivered message (body + key envelope properties) to JSON.
fn message_to_json(delivery: &lapin::message::Delivery) -> Value {
    let props = &delivery.properties;
    json!({
        "exchange": delivery.exchange.as_str(),
        "routingKey": delivery.routing_key.as_str(),
        "contentType": props.content_type().as_ref().map(|s| s.as_str()),
        "correlationId": props.correlation_id().as_ref().map(|s| s.as_str()),
        "body": decode_body(&delivery.data),
    })
}

/// Decode a message body: parsed JSON when it parses, else UTF-8 text, else
/// base64. The shape is stable: exactly one of `json` / `text` / `base64`.
fn decode_body(data: &[u8]) -> Value {
    if let Ok(v) = serde_json::from_slice::<Value>(data) {
        return json!({ "json": v });
    }
    match std::str::from_utf8(data) {
        Ok(s) => json!({ "text": s }),
        Err(_) => json!({ "base64": base64::engine::general_purpose::STANDARD.encode(data) }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_json_body() {
        let v = decode_body(br#"{"a":1}"#);
        assert_eq!(v["json"]["a"], json!(1));
    }

    #[test]
    fn decode_text_body() {
        let v = decode_body(b"hello");
        assert_eq!(v["text"], json!("hello"));
    }

    #[test]
    fn decode_binary_body_is_base64() {
        let v = decode_body(&[0xff, 0xfe, 0xfd]);
        assert_eq!(v["base64"], json!("//79"));
    }
}
