//! `watch_strategy` entity (`amqp_queue`): each message on a RabbitMQ /
//! AMQP 0.9.1 queue as an MCP Event.
//!
//! The strategy consumes the queue with manual acknowledgement and a
//! prefetch of `max_in_flight`, through the SDK's queue helper: a message is
//! acked once the gateway has stored its event, and nacked back onto the
//! queue when the gateway has not done so in `ack_timeout_ms`, so delivery
//! is at-least-once. The `message_id` property, when the publisher sets it,
//! is the event id, which the gateway takes once.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions, QueueBindOptions,
    QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{Acker, Connection, ConnectionProperties, Consumer};
use mcpg_plugin_protocol::backend::{WatchError, WatchEvent};
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::HostHandle;
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use mcpg_plugin_sdk::queue::{
    Delivery, QueueOptions, QueueSource, acknowledge_queue_watch, cancel_queue_watch, defaults,
    spawn_queue_watch,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

pub const PLUGIN_ID: &str = "dev.mcpg.backend.amqp";

/// The strategy discriminator this entity handles.
pub const WATCH_KIND: &str = "amqp_queue";

/// The event type each message makes.
pub const MESSAGE_EVENT_TYPE: &str = "amqp.message";

/// Largest `max_body_bytes`: the event, with its headers, must stay under
/// the host's 256 KiB.
const MAX_BODY_BYTES: usize = 192 * 1024;

/// How long one receive waits for a message.
const RECEIVE_WAIT: Duration = Duration::from_secs(1);

fn default_true() -> bool {
    true
}

/// A binding of the watched queue to an exchange.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingSpec {
    exchange: String,
    #[serde(default)]
    routing_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchSpec {
    /// `amqp://` or `amqps://` broker URI; its password resolves before the
    /// watch starts (`${env.X}`, `${secret.X}`, `vault://…`).
    uri: String,
    /// The queue to consume.
    queue: String,
    /// Declare the queue rather than require it to exist.
    #[serde(default)]
    declare: bool,
    /// A declared queue survives a broker restart.
    #[serde(default = "default_true")]
    durable: bool,
    /// Exchanges to bind the queue to before consuming.
    #[serde(default)]
    bindings: Vec<BindingSpec>,
    /// Most messages delivered and not yet acknowledged: the prefetch.
    #[serde(default = "defaults::max_in_flight")]
    max_in_flight: usize,
    /// How long a message waits for the gateway before it goes back onto
    /// the queue.
    #[serde(default = "defaults::ack_timeout_ms")]
    ack_timeout_ms: u64,
    /// A body past this is left out of the event, with its size.
    #[serde(default = "defaults::max_body_bytes")]
    max_body_bytes: usize,
}

/// The open connection a watch consumes over.
struct Live {
    connection: Connection,
    consumer: Consumer,
}

/// The broker side of one `amqp_queue` watch.
struct AmqpQueue {
    uri: String,
    queue: String,
    declare: bool,
    durable: bool,
    bindings: Vec<BindingSpec>,
    prefetch: u16,
    max_body_bytes: usize,
    rt: tokio::runtime::Runtime,
    live: Mutex<Option<Live>>,
    /// The acker of each delivery not yet settled, by receipt.
    ackers: Mutex<HashMap<u64, Acker>>,
    next_receipt: AtomicU64,
}

impl AmqpQueue {
    async fn open(&self) -> Result<Live, String> {
        let connection = Connection::connect(&self.uri, ConnectionProperties::default())
            .await
            .map_err(|e| format!("AMQP connect failed: {e}"))?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| format!("AMQP channel failed: {e}"))?;
        let options = QueueDeclareOptions {
            passive: !self.declare,
            durable: self.durable,
            ..QueueDeclareOptions::default()
        };
        channel
            .queue_declare(self.queue.as_str().into(), options, FieldTable::default())
            .await
            .map_err(|e| {
                if self.declare {
                    format!("AMQP declare of queue '{}' failed: {e}", self.queue)
                } else {
                    format!("AMQP queue '{}' is not there: {e}", self.queue)
                }
            })?;
        for binding in &self.bindings {
            channel
                .queue_bind(
                    self.queue.as_str().into(),
                    binding.exchange.as_str().into(),
                    binding.routing_key.as_str().into(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    format!(
                        "AMQP bind of queue '{}' to exchange '{}' failed: {e}",
                        self.queue, binding.exchange
                    )
                })?;
        }
        channel
            .basic_qos(self.prefetch, BasicQosOptions::default())
            .await
            .map_err(|e| format!("AMQP qos failed: {e}"))?;
        let consumer = channel
            .basic_consume(
                self.queue.as_str().into(),
                "".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| format!("AMQP consume of queue '{}' failed: {e}", self.queue))?;
        Ok(Live {
            connection,
            consumer,
        })
    }

    fn delivery(&self, delivery: lapin::message::Delivery) -> Delivery {
        let receipt = self.next_receipt.fetch_add(1, Ordering::Relaxed);
        let event = message_event(&self.queue, &delivery, self.max_body_bytes);
        self.ackers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(receipt, delivery.acker);
        Delivery {
            event,
            receipt: Value::from(receipt),
        }
    }

    fn acker(&self, receipt: &Value) -> Option<Acker> {
        let receipt = receipt.as_u64()?;
        self.ackers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&receipt)
    }
}

impl QueueSource for AmqpQueue {
    fn connect(&self) -> Result<(), String> {
        let mut live = self.live.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(old) = live.take() {
            let _ = self
                .rt
                .block_on(old.connection.close(200, "reconnecting".into()));
        }
        *live = Some(self.rt.block_on(self.open())?);
        Ok(())
    }

    fn receive(&self, max: usize, wait: Duration) -> Result<Vec<Delivery>, String> {
        let mut live = self.live.lock().unwrap_or_else(|e| e.into_inner());
        let Some(live) = live.as_mut() else {
            return Err("not connected".into());
        };
        let received = self.rt.block_on(async {
            let mut received = Vec::new();
            match tokio::time::timeout(wait, live.consumer.next()).await {
                Err(_) => return Ok(received),
                Ok(None) => return Err("the broker ended the consumer".to_owned()),
                Ok(Some(Err(e))) => return Err(format!("AMQP delivery failed: {e}")),
                Ok(Some(Ok(delivery))) => received.push(delivery),
            }
            // What the prefetch already holds, without waiting.
            while received.len() < max {
                match tokio::time::timeout(Duration::ZERO, live.consumer.next()).await {
                    Ok(Some(Ok(delivery))) => received.push(delivery),
                    _ => break,
                }
            }
            Ok(received)
        })?;
        Ok(received
            .into_iter()
            .map(|delivery| self.delivery(delivery))
            .collect())
    }

    fn ack(&self, receipt: &Value) -> Result<(), String> {
        let Some(acker) = self.acker(receipt) else {
            return Ok(());
        };
        self.rt
            .block_on(acker.ack(BasicAckOptions::default()))
            .map(|_| ())
            .map_err(|e| format!("AMQP ack failed: {e}"))
    }

    fn release(&self, receipt: &Value) -> Result<(), String> {
        let Some(acker) = self.acker(receipt) else {
            return Ok(());
        };
        let options = BasicNackOptions {
            multiple: false,
            requeue: true,
        };
        self.rt
            .block_on(acker.nack(options))
            .map(|_| ())
            .map_err(|e| format!("AMQP nack failed: {e}"))
    }
}

impl Drop for AmqpQueue {
    fn drop(&mut self) {
        let live = self.live.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some(live) = live.take() {
            let _ = self
                .rt
                .block_on(live.connection.close(200, "watch cancelled".into()));
        }
    }
}

/// One header or table value as JSON: strings as text when they are UTF-8,
/// bytes as standard base64, a timestamp as RFC 3339.
fn amqp_value(value: &AMQPValue) -> Value {
    let base64 = |bytes: &[u8]| json!(base64::engine::general_purpose::STANDARD.encode(bytes));
    match value {
        AMQPValue::Boolean(b) => json!(b),
        AMQPValue::ShortShortInt(i) => json!(i),
        AMQPValue::ShortShortUInt(i) => json!(i),
        AMQPValue::ShortInt(i) => json!(i),
        AMQPValue::ShortUInt(i) => json!(i),
        AMQPValue::LongInt(i) => json!(i),
        AMQPValue::LongUInt(i) => json!(i),
        AMQPValue::LongLongInt(i) => json!(i),
        AMQPValue::Float(f) => json!(f),
        AMQPValue::Double(d) => json!(d),
        AMQPValue::DecimalValue(d) => {
            json!(f64::from(d.value) / 10f64.powi(i32::from(d.scale)))
        }
        AMQPValue::ShortString(s) => json!(s.as_str()),
        AMQPValue::LongString(s) => match std::str::from_utf8(s.as_bytes()) {
            Ok(text) => json!(text),
            Err(_) => base64(s.as_bytes()),
        },
        AMQPValue::FieldArray(items) => {
            Value::Array(items.as_slice().iter().map(amqp_value).collect())
        }
        AMQPValue::Timestamp(t) => i64::try_from(*t)
            .ok()
            .and_then(|secs| {
                mcpg_plugin_protocol::occurred_at_from_millis(secs.saturating_mul(1_000))
            })
            .map_or(Value::Null, Value::from),
        AMQPValue::FieldTable(table) => field_table(table),
        AMQPValue::ByteArray(bytes) => base64(bytes.as_slice()),
        AMQPValue::Void => Value::Null,
    }
}

fn field_table(table: &FieldTable) -> Value {
    Value::Object(
        table
            .into_iter()
            .map(|(key, value)| (key.as_str().to_owned(), amqp_value(value)))
            .collect(),
    )
}

/// The `amqp.message` event of one delivery.
fn message_event(
    queue: &str,
    delivery: &lapin::message::Delivery,
    max_body_bytes: usize,
) -> WatchEvent {
    let props = &delivery.properties;
    let mut data = Map::new();
    data.insert("queue".into(), json!(queue));
    data.insert("exchange".into(), json!(delivery.exchange.as_str()));
    data.insert("routingKey".into(), json!(delivery.routing_key.as_str()));
    data.insert("redelivered".into(), json!(delivery.redelivered));
    let strings = [
        ("messageId", props.message_id()),
        ("correlationId", props.correlation_id()),
        ("replyTo", props.reply_to()),
        ("contentType", props.content_type()),
        ("contentEncoding", props.content_encoding()),
        ("type", props.kind()),
        ("appId", props.app_id()),
        ("userId", props.user_id()),
        ("expiration", props.expiration()),
    ];
    for (name, value) in strings {
        if let Some(value) = value {
            data.insert(name.into(), json!(value.as_str()));
        }
    }
    if let Some(priority) = props.priority() {
        data.insert("priority".into(), json!(priority));
    }
    if let Some(mode) = props.delivery_mode() {
        data.insert("deliveryMode".into(), json!(mode));
    }
    let occurred_at = props
        .timestamp()
        .and_then(|secs| i64::try_from(secs).ok())
        .and_then(|secs| mcpg_plugin_protocol::occurred_at_from_millis(secs.saturating_mul(1_000)));
    if let Some(at) = &occurred_at {
        data.insert("timestamp".into(), json!(at));
    }
    data.insert(
        "headers".into(),
        props
            .headers()
            .as_ref()
            .map_or_else(|| json!({}), field_table),
    );
    if delivery.data.len() > max_body_bytes {
        data.insert("body".into(), Value::Null);
        data.insert("bodyEncoding".into(), Value::Null);
        data.insert("bodyTruncated".into(), json!(true));
        data.insert("bodySize".into(), json!(delivery.data.len()));
    } else {
        let (body, encoding) = mcpg_plugin_protocol::event_body(&delivery.data);
        data.insert("body".into(), body);
        data.insert("bodyEncoding".into(), json!(encoding));
    }
    WatchEvent {
        event_id: props
            .message_id()
            .as_ref()
            .map(|id| id.as_str().to_owned())
            .filter(|id| !id.is_empty()),
        event_type: Some(MESSAGE_EVENT_TYPE.to_owned()),
        occurred_at,
        data: Some(Value::Object(data)),
        ..WatchEvent::default()
    }
}

/// `watch_strategy` entity. Stateless beyond its manifest — each watcher's
/// broker and queue arrive on the per-watch spec.
pub struct AmqpWatchCdylib {
    manifest: PluginManifest,
}

impl AmqpWatchCdylib {
    /// Infallible cdylib factory; the watch carries no plugin-level config.
    pub fn from_host_config(_config_json: &str, _host: HostHandle) -> Self {
        crate::install_crypto_provider();
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.backend.amqp",
                name: "AMQP Queue Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

impl SyncWatchStrategyPlugin for AmqpWatchCdylib {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        WATCH_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let (spec, _) = mcpg_plugin_protocol::take_watch_resume(spec);
        let parsed: WatchSpec =
            serde_json::from_value(spec).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid amqp_queue watch spec: {e}"),
            })?;
        let invalid = |m: String| WatchError::InvalidSpec { message: m };
        if !parsed.uri.starts_with("amqp://") && !parsed.uri.starts_with("amqps://") {
            return Err(invalid("uri must start with amqp:// or amqps://".into()));
        }
        if parsed.uri.contains("cred://") {
            return Err(invalid(
                "uri must not contain cred:// — per-caller credentials are unsupported; use \
                 ${env.X} / ${secret.X} / vault:// (resolved before the watch starts)"
                    .into(),
            ));
        }
        if parsed.queue.trim().is_empty() {
            return Err(invalid("queue must not be empty".into()));
        }
        if parsed.bindings.iter().any(|b| b.exchange.trim().is_empty()) {
            return Err(invalid("a binding needs an exchange".into()));
        }
        if !(1..=MAX_BODY_BYTES).contains(&parsed.max_body_bytes) {
            return Err(invalid(format!(
                "max_body_bytes must be 1 to {MAX_BODY_BYTES}"
            )));
        }
        let options = QueueOptions {
            source: format!("amqp queue {}", parsed.queue),
            max_in_flight: parsed.max_in_flight,
            ack_timeout: Duration::from_millis(parsed.ack_timeout_ms),
            receive_wait: RECEIVE_WAIT,
        };
        options.validate().map_err(invalid)?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("mcpg-amqp-watch")
            .enable_all()
            .build()
            .map_err(|e| WatchError::Subscribe {
                message: format!("amqp_queue: tokio runtime init failed: {e}"),
            })?;
        let source = AmqpQueue {
            uri: parsed.uri,
            queue: parsed.queue,
            declare: parsed.declare,
            durable: parsed.durable,
            bindings: parsed.bindings,
            prefetch: u16::try_from(parsed.max_in_flight).unwrap_or(u16::MAX),
            max_body_bytes: parsed.max_body_bytes,
            rt,
            live: Mutex::new(None),
            ackers: Mutex::new(HashMap::new()),
            next_receipt: AtomicU64::new(0),
        };
        spawn_queue_watch(resource_uri, options, emit_event, source)
    }

    fn acknowledge(&self, watch_handle: &WatchHandleBox, position: &Value) {
        acknowledge_queue_watch(watch_handle, position);
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        cancel_queue_watch(watch_handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stub_host() -> HostHandle {
        // SAFETY: `stub_host_ref` returns a process-static no-op host ref; the
        // factory ignores the host entirely.
        #[allow(unsafe_code)]
        unsafe {
            HostHandle::from_ffi(mcpg_plugin_sdk::testing::stub_host_ref())
        }
    }

    fn plugin() -> AmqpWatchCdylib {
        AmqpWatchCdylib::from_host_config("", stub_host())
    }

    #[test]
    fn a_tls_configuration_can_be_built_once_the_plugin_is_made() {
        let _ = plugin();
        let _ = rustls::ClientConfig::builder();
    }

    fn refused(spec: Value) -> String {
        match plugin().watch("mcpg-event://orders", &spec, Box::new(|_| {})) {
            Err(WatchError::InvalidSpec { message }) => message,
            Err(other) => panic!("not a spec error: {other:?}"),
            Ok(handle) => {
                plugin().cancel(handle);
                panic!("accepted {spec}");
            }
        }
    }

    #[test]
    fn manifest_and_kind_are_correct() {
        use mcpg_plugin_protocol::PluginClass;
        let p = plugin();
        assert_eq!(SyncWatchStrategyPlugin::manifest(&p).id, PLUGIN_ID);
        assert_eq!(
            SyncWatchStrategyPlugin::manifest(&p).plugin_class,
            PluginClass::WatchStrategy
        );
        assert_eq!(p.kind(), WATCH_KIND);
    }

    #[test]
    fn spec_parses_with_defaults() {
        let parsed: WatchSpec = serde_json::from_value(json!({
            "uri": "amqp://guest:guest@localhost:5672/%2f",
            "queue": "orders",
        }))
        .unwrap();
        assert!(!parsed.declare);
        assert!(parsed.durable);
        assert!(parsed.bindings.is_empty());
        assert_eq!(parsed.max_in_flight, 256);
        assert_eq!(parsed.ack_timeout_ms, 60_000);
        assert_eq!(parsed.max_body_bytes, 128 * 1024);
    }

    #[test]
    fn a_bad_spec_is_refused_before_anything_connects() {
        let base = json!({ "uri": "amqp://localhost", "queue": "orders" });
        let with = |extra: Value| {
            let mut spec = base.clone();
            for (k, v) in extra.as_object().unwrap() {
                spec[k] = v.clone();
            }
            spec
        };
        assert!(refused(with(json!({ "bogus": 1 }))).contains("unknown field"));
        let scheme = refused(with(json!({ "uri": "http://user:secret@h" })));
        assert!(scheme.contains("amqp://"));
        assert!(!scheme.contains("secret"), "the URI is not echoed");
        assert!(refused(with(json!({ "uri": "amqp://u:cred://x@h" }))).contains("cred://"));
        assert!(refused(with(json!({ "queue": " " }))).contains("queue"));
        assert!(refused(with(json!({ "bindings": [{ "exchange": "" }] }))).contains("exchange"));
        assert!(refused(with(json!({ "max_in_flight": 0 }))).contains("max_in_flight"));
        assert!(refused(with(json!({ "ack_timeout_ms": 0 }))).contains("ack_timeout_ms"));
        assert!(refused(with(json!({ "max_body_bytes": 300_000 }))).contains("max_body_bytes"));
    }

    #[test]
    fn header_values_become_json() {
        let mut table = FieldTable::default();
        table.insert("text".into(), AMQPValue::LongString("hi".into()));
        table.insert("n".into(), AMQPValue::LongLongInt(-3));
        table.insert("ok".into(), AMQPValue::Boolean(true));
        table.insert(
            "bytes".into(),
            AMQPValue::ByteArray(vec![0xff, 0xfe].into()),
        );
        table.insert(
            "list".into(),
            AMQPValue::FieldArray(vec![AMQPValue::ShortInt(1), AMQPValue::Void].into()),
        );
        table.insert("at".into(), AMQPValue::Timestamp(1_700_000_000));
        let json = field_table(&table);
        assert_eq!(json["text"], json!("hi"));
        assert_eq!(json["n"], json!(-3));
        assert_eq!(json["ok"], json!(true));
        assert_eq!(json["bytes"], json!("//4="));
        assert_eq!(json["list"], json!([1, null]));
        assert_eq!(json["at"], json!("2023-11-14T22:13:20.000Z"));
    }
}
