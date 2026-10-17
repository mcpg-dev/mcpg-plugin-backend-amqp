# `mcpg-plugin-backend-amqp`

AMQP 0.9.1 (RabbitMQ) backend binding plugin for mcpg (`kind: amqp`).
Publishes messages, runs request/reply (RPC), or pulls one message from a
queue, as MCP **tools** and **resources** — over a reused lapin connection
(rustls TLS). The message body is the tool's arguments JSON.

Part of the legacy → MCP bridge suite.
Complements the `kafka` / `nats` messaging backends. IBM MQ (FFI) is
deferred.

## How it works

One binding = one operation = one MCP tool (or resource). The `op` selects
the behaviour:

| `op` | Behaviour | Returns |
|---|---|---|
| `publish` (default) | Publish to `exchange`/`routing_key`, with a publisher confirm + a mandatory-routing check. | `{ published: true }` — error if nacked or unroutable. |
| `rpc` | Publish with `reply_to` + `correlation_id`, then await the correlated reply on a private exclusive queue (bounded by `timeout_ms`). | `{ message: <reply body> }` |
| `get` | `basic_get` one message from `queue` (auto-acked). | `{ found, message }` — `found:false` on an empty queue. |

The message body is the call's arguments JSON (the request payload). Replies
and fetched messages are decoded as exactly one of `json` / `text` /
`base64`. The connection is established lazily and reused across calls
(reconnecting if it drops).

## Configuration

| Field | Type | Default | Notes |
|---|---|---|---|
| `uri` | string (required) | — | `amqp://user:pass@host:5672/vhost` or `amqps://…`. Operator-configured; the password resolves via the gateway secret-resolver (`${env.X}` / `vault://…`). Per-caller `cred://` is **not** supported. |
| `op` | `publish`\|`rpc`\|`get` | `publish` | The operation. |
| `exchange` | string | `""` | Target exchange. Empty = the default direct exchange (`routing_key` is the queue name). |
| `routing_key` | string | `""` | Routing key for `publish`/`rpc`. Operator-fixed. |
| `queue` | string | `""` | Source queue for `get`. |
| `content_type` | string | `application/json` | Stamped on published messages. |
| `timeout_ms` | int | `10000` | connect + publish + (RPC) reply timeout. |

### As a publish tool

```yaml
mcp:
  capabilities:
    tools:
      - name: jobs.enqueue
        description: Enqueue a background job.
        input_schema:
          type: object
          properties: { task: { type: string }, payload: { type: object } }
          required: [task]
        backend:
          kind: amqp
          uri: "amqp://mcpg:${env.RABBIT_PASSWORD}@rabbit.internal:5672/%2f"
          op: publish
          routing_key: "jobs"          # default exchange → queue "jobs"
```

### As an RPC tool

```yaml
      backend:
        kind: amqp
        uri: "amqps://mcpg:${env.RABBIT_PASSWORD}@rabbit.internal:5671/%2f"
        op: rpc
        routing_key: "rpc.pricing"   # a service consuming this queue replies
        timeout_ms: 8000
```

## Response envelope

```jsonc
{
  "toolName": "jobs.enqueue",
  "profile":  "jobs.enqueue",
  "request":  { "op": "publish", "exchange": "", "routingKey": "jobs", "queue": "" },
  "response": {                       // shape depends on op
    "published": true,                // publish
    "message": null,                  // rpc → reply body; get → message or null
    "found": null,                    // get → true/false
    "durationMs": 4
  },
  "downstreamError": null,            // non-null ⇒ isError:true (amqp_error / transport_error)
  "downstreamErrors": [],
  "error": null
}
```

A fetched/replied message decodes to one of:

```jsonc
"message": {
  "exchange": "", "routingKey": "jobs",
  "contentType": "application/json", "correlationId": null,
  "body": { "json": { "task": "build" } }   // or { "text": "…" } / { "base64": "…" }
}
```

## Security

- **No plaintext secrets.** The broker `uri`/password resolves through the
  gateway secret-resolver (`${env.X}` / `vault://…`); it is never committed.
- **`cred://` not supported.** The connection is per-binding (one identity),
  so per-caller `cred://` is rejected at config validation.
- **Mandatory routing.** `publish`/`rpc` set the mandatory flag, so an
  unroutable message is reported as an error rather than silently dropped.
- **TLS.** `amqps://` over rustls (lapin's default rustls feature — modern
  rustls 0.23, native-tls is banned). The broker certificate is validated
  against the system roots.

## Queue watch (`amqp_queue`)

A second entity, a `watch_strategy` of kind `amqp_queue`, consumes a queue
as an MCP Events source: each message is one `amqp.message` event. The
consumer uses manual acknowledgement. A message is acked once the gateway
has stored its event, and nacked back onto the queue when the gateway has
not done so in `ack_timeout_ms`, so delivery is at-least-once. When the
watch stops, the messages it holds go back onto the queue at once.

```yaml
mcp:
  capabilities:
    events:
      - name: orders.placed
        description: An order message from the orders exchange.
        source:
          watch:
            strategy:
              type: plugin
              kind: amqp_queue
              uri: "amqps://mcpg:${env.RABBITMQ_PASSWORD}@rabbit.internal:5671/%2f"
              queue: mcpg-orders
              declare: true
              bindings:
                - exchange: orders
                  routing_key: "order.*"
            event_type: amqp.message
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `uri` | string (required) | — | `amqp://` or `amqps://`; the password resolves before the watch starts (`${env.X}`, `${secret.X}`, `vault://…`). `cred://` is rejected. |
| `queue` | string (required) | — | The queue to consume. |
| `declare` | bool | `false` | Declare the queue. Otherwise it must exist, and a missing queue is reported and tried again. |
| `durable` | bool | `true` | A declared queue survives a broker restart. |
| `bindings` | list of `{exchange, routing_key}` | `[]` | Bind the queue to these exchanges before consuming. |
| `max_in_flight` | int | `256` | The prefetch: most messages delivered and not yet acknowledged (1 to 1000). |
| `ack_timeout_ms` | int | `60000` | How long a message waits for the gateway before it goes back onto the queue. |
| `max_body_bytes` | int | `131072` | A larger body is left out of the event, with `bodyTruncated: true` and `bodySize` (at most 196608). |

Each event's `data` carries `queue`, `exchange`, `routingKey`,
`redelivered`, the message properties that are set (`messageId`,
`correlationId`, `replyTo`, `contentType`, `contentEncoding`, `type`,
`appId`, `userId`, `expiration`, `priority`, `deliveryMode`, `timestamp`),
`headers` as JSON, and `body` with `bodyEncoding`: `json`, `text` or
`base64`. The `message_id` property is the event id, so a message delivered
twice is stored once; set it on the publisher. A lost connection is opened
again with backoff; the broker requeues what the old one held.

## Build / test

```bash
cargo build  -p mcpg-plugin-backend-amqp
cargo test   -p mcpg-plugin-backend-amqp                              # unit tests
cargo test   -p mcpg-plugin-backend-amqp --features integration-tests # RabbitMQ (docker)
cargo clippy -p mcpg-plugin-backend-amqp --all-targets -- -D warnings
```

The integration suite drives a publish → get round-trip, an RPC call, and
an `amqp_queue` watch through bindings, the prefetch window, an
acknowledgement, a redelivery after the timeout, and a release on cancel.

## Scope / deferred

- **Per-caller credentials** (`cred://`, per-cred connection cache) — v1 is
  one reused connection per binding.
- **IBM MQ** — planned as a separate plugin (FFI / `libmqm`).
- **Topology declaration** for the tools (declaring exchanges/queues/bindings
  from config) — the tools assume the topology already exists; the queue
  watch can declare its queue and bindings.
