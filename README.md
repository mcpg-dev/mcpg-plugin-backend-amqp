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

## Build / test

```bash
nx build mcpg-plugin-backend-amqp
nx test  mcpg-plugin-backend-amqp                                   # unit tests
cargo test -p mcpg-plugin-backend-amqp --features integration-tests  # RabbitMQ (docker)
nx lint  mcpg-plugin-backend-amqp
```

## Scope / deferred

- **Queue-watch resource** (a streaming subscription surfaced as an MCP
  resource) — v1 is `publish` / `rpc` / `get` (single-shot).
- **Per-caller credentials** (`cred://`, per-cred connection cache) — v1 is
  one reused connection per binding.
- **IBM MQ** — planned as a separate plugin (FFI / `libmqm`).
- **Topology declaration** (declaring exchanges/queues/bindings from config)
  — v1 assumes the topology already exists.
