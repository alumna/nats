# Alumna NATS

[![Crystal CI](https://github.com/alumna/nats/actions/workflows/ci.yml/badge.svg)](https://github.com/alumna/nats/actions/workflows/ci.yml) ![Dynamic YAML Badge](https://img.shields.io/badge/dynamic/yaml?url=https%3A%2F%2Fraw.githubusercontent.com%2Falumna%2Fnats%2Frefs%2Fheads%2Fmaster%2Fshard.yml&query=version&prefix=v&label=version) ![GitHub License](https://img.shields.io/github/license/alumna/nats)

NATS for the [Alumna Backend Framework](https://github.com/alumna/backend).

`Alumna::Nats` holds one NATS client for the process. Use it to:

- publish and subscribe across processes
- run competing workers on a core queue group (no persist)
- run a durable job queue on JetStream workqueue (ack)

This shard is not a Service adapter. It does not import HTTP WebSocket. Combine NATS and local Connections in the application if you need browser push. See [Examples](#9-examples) and [WebSocket fan-out](#10-websocket-fan-out).

See [ROADMAP.md](ROADMAP.md).

| If you need | Use |
|---|---|
| Every live subscriber gets a copy. No store. | `subscribe` with no queue group |
| One worker in a group gets each message. No store. | `subscribe` with `queue_group:` |
| A job waits for a worker. The first ack removes it. | JetStream stream with `retention: :workqueue` |
| Stored copies for independent consumers | JetStream `:limits` or `:interest`. Do not use `:workqueue`. |

---

## Table of Contents
1. [Installation](#1-installation)
2. [Connect](#2-connect)
3. [Publish and subscribe](#3-publish-and-subscribe)
4. [Queue groups](#4-queue-groups)
5. [JetStream jobs](#5-jetstream-jobs)
6. [Errors](#6-errors)
7. [Security](#7-security)
8. [Testing](#8-testing)
9. [Examples](#9-examples)
10. [WebSocket fan-out](#10-websocket-fan-out)
11. [License](#11-license)

---

## 1. Installation

Add it to your `shard.yml`:

```yaml
dependencies:
  alumna:
    github: alumna/backend
    version: ~> 0.9.1
  alumna-nats:
    github: alumna/nats
```

Then run `shards install`.

Needs Alumna Backend **0.9.1** or later (`after_commit` for WebSocket fan-out).

Install a NATS server. The default URL is `nats://127.0.0.1:4222`. Core publish and subscribe need that server.

JetStream jobs need JetStream on the server. Start the server with `nats-server -js`, or set `jetstream {}` in the server config.

For a local unpublished backend clone, use gitignored `shard.override.yml`:

```yaml
dependencies:
  alumna:
    path: ../backend
```

---

## 2. Connect

```crystal
require "alumna-nats"

nats = Alumna::Nats.new(URI.parse(ENV["NATS_URL"]))
if nats.is_a?(Alumna::Nats::Error)
  # Handle the connect failure. The message has no URI userinfo.
else
  nats.ping
  nats.flush
  nats.close
end
```

`Alumna::Nats.new` accepts a `URI`, a URL string, or an array of servers. `from_uri` is the same as `new` with one URI or string.

```crystal
nats = Alumna::Nats.new("nats://127.0.0.1:4222")
nats = Alumna::Nats.from_uri(URI.parse(ENV["NATS_URL"]))
nats = Alumna::Nats.new([
  "nats://127.0.0.1:4222",
  "nats://127.0.0.1:4223",
])
```

Optional `nkeys_file` and `user_credentials` go to the driver:

```crystal
nats = Alumna::Nats.new(
  URI.parse(ENV["NATS_URL"]),
  nkeys_file: "user.nk",
  user_credentials: "user.creds",
)
```

| Scheme | Transport |
|---|---|
| `nats://` | TCP |
| `tls://` | TLS |

User and password in the URI are NATS AUTH. A bad scheme or an empty server list raises `ArgumentError`. There is no Unix socket.

From the environment (default `NATS_URL`):

```crystal
nats = Alumna::Nats.from_env
# or:
nats = Alumna::Nats.from_env("NATS_URL")
```

Use one `Alumna::Nats` per process. Do not open a client per request.

---

## 3. Publish and subscribe

The application owns the subject name. There is no required prefix. Payload is `String` or `Bytes`. See [Efficient use](#efficient-use) for `publish`, `publish_json`, and `payload`.

```crystal
sub = nats.subscribe("orders.created") do |msg|
  body = msg.payload
end
if sub.is_a?(Alumna::Nats::Error)
  # Handle the subscribe failure.
else
  nats.publish("orders.created", %({"id":1}))
  nats.flush
  nats.unsubscribe(sub)
end
```

`subscribe` does not block. It returns `Alumna::Nats::Subscription` or `Alumna::Nats::Error`. Pass the handle to `unsubscribe`.

### Efficient use

`publish` sends a `String` or `Bytes` unchanged. Use it for text that is already encoded, including a JSON string you already built.

`publish_json` encodes `AnyData` into a reused buffer and publishes those bytes. A hash or a list does not allocate a JSON string on each call. Use it for a hash, an array, a number, a bool, a time, or nil.

```crystal
order = {} of String => Alumna::AnyData
order["id"] = 1_i64
nats.publish_json("orders.created", order)
```

A `String` passed to `publish_json` is encoded as a JSON string. The server text has quotes and escapes. JSON text you already have goes to `publish`.

In the handler, read `msg.payload`. That string is the server text. It stays valid for as long as you keep it.

```crystal
nats.subscribe("orders.created") do |msg|
  data = Alumna::JsonHelper.from_string(msg.payload)
end
```

`msg.body` is a byte view of `payload`. Copy `body` when you store the bytes and discard the message. `String.new(msg.body)` copies the whole payload. Use `payload` instead.

`nats.jetstream` returns the same helper for that client on every call. Keep that value next to the client.

`publish` and `publish_json` stay in the client buffer until `flush`. After a burst, call `flush` once.

Do not call `publish_json` from this client's disconnect handler. The encode lock is not reentrant.

Each current subscriber on a subject gets a copy of the message. If no subscriber is connected, the server does not keep the message. This is not a durable job queue.

Subscribe may use NATS wildcards: `*` for one token, `>` for the rest. Publish must use a concrete subject.

`publish` writes to a buffer. Call `flush` when the process must send the data now. `close` also flushes.

Empty subject raises `ArgumentError`. A subject with a space, a NUL byte, or (on publish) `*` or `>` also raises `ArgumentError`.

Core `nats.publish` does not use JetStream. Core publish does not create a stream.

---

## 4. Queue groups

A queue group makes subscribers compete. Each message goes to one subscriber in the group. Core NATS does not store the message. This is still not a durable job queue.

```crystal
sub = nats.subscribe("jobs.email", queue_group: "workers") do |msg|
  body = msg.payload
end
```

Two queue groups on the same subject each get a copy. Workers inside one group compete.

`Subscription.queue_group` is the group name, or `nil` when the subscribe has no group.

Empty queue group raises `ArgumentError`.

---

## 5. JetStream jobs

You must create a stream. Publish does not create a stream.

You must create a durable push consumer. Subscribe does not create a consumer. The handler does not ack.

For a job queue, pass `retention: :workqueue`. The default retention is `:limits`.

`:workqueue` is the job queue. The first ack removes the message. One durable consumer (and its deliver group) receives each message. A second consumer on the same interest returns `Alumna::Nats::Error`. Two workers on that consumer compete.

Do not use `:workqueue` for stored copies to many independent consumers. Use `:limits` or `:interest`.

```crystal
js = nats.jetstream

stream = js.create_stream("jobs", ["jobs.email"], storage: :file, retention: :workqueue)
if stream.is_a?(Alumna::Nats::Error)
  # Handle the create failure.
else
  consumer = js.create_consumer("jobs", "workers", ack_wait: 5.seconds)
  if consumer.is_a?(Alumna::Nats::Error)
    # Handle the create failure.
  else
    sub = js.subscribe(consumer) do |msg|
      body = msg.payload
      js.ack(msg)
      # or: js.nack(msg)
      # or: js.nack(msg, delay: 1.second)
    end
    ack = js.publish("jobs.email", %({"to":"a@example.com"}))
    nats.flush
    js.unsubscribe(sub)
    js.delete_consumer("jobs", "workers")
    js.delete_stream("jobs")
  end
end
```

`create_stream` returns `Alumna::Nats::JetStream::Stream` or `Alumna::Nats::Error`. If the stream already exists with the same name, `create_stream` returns that stream. Two streams must not share a subject.

`stream_info` returns the stream, `nil` if the stream does not exist, or `Alumna::Nats::Error`. `delete_stream` removes the stream. If the stream does not exist, `delete_stream` is a no-op.

`js.publish` stores the message in a stream that already listens on that subject. If no stream listens, the call returns `Alumna::Nats::Error`. The process stays up. The shard does not create a stream.

Default storage is file. Pass `storage: :memory` for an in-memory stream. Optional `retention:` is `:limits` (default), `:interest`, or `:workqueue`.

`:limits` keeps the message until size or age limits. Independent consumers can each receive a copy. `:interest` keeps a message while a consumer is interested. If no consumer exists at publish time, `:interest` does not keep the message.

`create_consumer` returns `Alumna::Nats::JetStream::Consumer` or `Alumna::Nats::Error`. The consumer is durable. Ack policy is explicit. Deliver policy is all: a consumer created after publish still receives stored messages. Default deliver group is the consumer name, so workers on that consumer compete. Default deliver subject is generated from the stream name and the consumer name. You may pass `deliver_subject`, `deliver_group`, `filter_subject`, or `ack_wait`. `ack_wait` is how long an unacked message waits before the server delivers it again. On a workqueue stream, create one consumer per interest. Extra overlapping consumers return `Alumna::Nats::Error`.

`consumer_info` returns the consumer, `nil` if the consumer does not exist, or `Alumna::Nats::Error`. `delete_consumer` removes the consumer. If the consumer does not exist, `delete_consumer` is a no-op.

`js.subscribe` does not block. It returns `Alumna::Nats::Subscription` or `Alumna::Nats::Error`. It does not create a consumer. It does not ack when the handler returns. You call `ack` or `nack`. `nack` with `delay:` waits before the next delivery. Read `msg.payload`, same as core. See [Efficient use](#efficient-use).

This product uses push consumers. A consumer without a deliver subject raises `ArgumentError`.

Stream names must not be empty and must not contain `.`. A stream must have at least one subject. Empty stream name, empty subject list, or empty subject raises `ArgumentError`. Empty consumer name or a name with `.` raises `ArgumentError`. Empty deliver subject, empty deliver group, or empty filter subject raises `ArgumentError`. Empty JetStream ack subject raises `ArgumentError`. A nack delay that is not greater than zero raises `ArgumentError`. An ack wait that is not greater than zero raises `ArgumentError`.

`ack` and `nack` write to a buffer. Call `flush` when the process must send the data now.

---

## 6. Errors

Two channels. Do not mix them.

| Kind | Type | When |
|---|---|---|
| Operation | struct `Alumna::Nats::Error` | Driver or server failure. The process stays up. |
| Config | `ArgumentError` (raise) | Empty name, bad URL, invalid subject. Fix the call. |

`Error` is not an `Exception`. The message never includes URI userinfo (user and password).

```crystal
result = nats.publish("orders.created", payload)
if result.is_a?(Alumna::Nats::Error)
  # Handle the operation failure.
end
```

In the tables below, `Error` is `Alumna::Nats::Error`. JetStream types are under `Alumna::Nats::JetStream`.

### Return types

**Connect**

| Method | Type |
|---|---|
| `new`, `from_uri`, `from_env` | `Alumna::Nats \| Error` |

**Core**

| Method | Type |
|---|---|
| `subscribe` | `Subscription \| Error` |
| `publish`, `publish_json`, `unsubscribe`, `ping`, `flush`, `close` | `Nil \| Error` |

**JetStream**

| Method | Type |
|---|---|
| `create_stream` | `Stream \| Error` |
| `stream_info` | `Stream \| Nil \| Error` |
| `js.publish` | `PubAck \| Error` |
| `create_consumer` | `Consumer \| Error` |
| `consumer_info` | `Consumer \| Nil \| Error` |
| `js.subscribe` | `Subscription \| Error` |
| `delete_stream`, `delete_consumer`, `js.unsubscribe`, `ack`, `nack` | `Nil \| Error` |

`stream_info` and `consumer_info` return `nil` when the name does not exist. `delete_stream` and `delete_consumer` of a missing name are a no-op. `js.publish` with no stream returns `Error`. The process stays up.

### `ArgumentError`

These calls raise. They do not return `Error`.

| Mistake | Methods |
|---|---|
| Empty URL or empty server list | `new`, `from_uri` |
| Scheme is not `nats://` or `tls://` | `new`, `from_uri`, `from_env` |
| Missing or empty environment variable | `from_env` |
| Empty subject | `publish`, `publish_json`, `subscribe`, `js.publish`, `create_stream` |
| JSON number is not finite | `publish_json` |
| Invalid subject (space, NUL; `*` or `>` on publish) | `publish`, `subscribe` |
| Empty queue group | `subscribe` |
| Empty stream name, or a name that contains `.` | stream and consumer helpers |
| Empty stream subject list | `create_stream` |
| Empty consumer name, or a name that contains `.` | consumer helpers |
| Empty deliver subject, deliver group, or filter subject | `create_consumer` |
| Pull consumer (no deliver subject) | `js.subscribe` |
| Empty JetStream ack subject | `ack`, `nack` |
| `nack` delay not greater than zero | `nack` |
| `ack_wait` not greater than zero | `create_consumer` |

### Subscribe handlers

A return type cannot replace an exception in a subscribe handler. The handler must not raise. An uncaught raise goes to the NATS driver `on_error` (default no-op).

---

## 7. Security

- Do not log the NATS URI. It may contain a password.
- `Alumna::Nats::Error` strips `//user:pass@` from messages.
- Put the URI in `NATS_URL`. Do not commit a password.
- Do not commit `nkeys_file` or `user_credentials` files.
- Use `tls://` when the link is not trusted.
- Use one client per process.

---

## 8. Testing

Specs need a NATS server. Set `NATS_URL` or use `nats://127.0.0.1:4222`. JetStream examples need JetStream on that server. If NATS is down, the spec process stops with a clear message.

```bash
crystal spec
crystal spec -Dpreview_mt -Dexecution_context
```

GitHub Actions:

- Format check (`lint`)
- Specs against one NATS 2.10 server with JetStream on port **4222** (`test`)
- kcov on `src/` (line-rate 1.000) against that server (`coverage`)

`preview_mt` + `execution_context` runs on the spec jobs.

---

## 9. Examples

Teaching programs in `examples/`. They need a NATS server (`NATS_URL` or `nats://127.0.0.1:4222`). JetStream jobs need JetStream on that server.

| File | What it shows |
|---|---|
| `examples/pubsub.cr` | Core publish and subscribe. Every current subscriber gets a copy. |
| `examples/jobs.cr` | Core queue group, then a JetStream workqueue job (ack). |
| `examples/websocket_fanout.cr` | `after_commit` publish → NATS subscribe → local `Connections.send_topic` |

```bash
crystal run examples/pubsub.cr
crystal run examples/jobs.cr
crystal run examples/websocket_fanout.cr
```

`websocket_fanout.cr` listens on port **3000**. Pass `--check` to run one create through NATS and confirm the WebSocket push, then exit.

---

## 10. WebSocket fan-out

Alumna Backend holds sockets on **this process** (`App#connections`). This shard is the bus between processes. The application combines them. This shard does not import HTTP WebSocket. Backend does not import NATS.

Use core `subscribe` with **no** queue group so every process that holds sockets gets a copy. Do not use JetStream workqueue for browser push. Workqueue is the job queue.

The application owns subject names. A convention that matches mutation events:

- Publish `messages.created`, `messages.updated`, `messages.patched`, `messages.removed`.
- Subscribe `messages.>`.
- Call `send_topic("messages", msg.payload)` so one `watch("messages")` receives every mutation. `payload` is the server text. No extra copy.
- Encode JSON with `event`, `path`, and `result`. Do not use a request/reply `id` on a push.

`watch` is not a WebSocket frame. A rule can call `connections.watch` with `ctx.store["connection_id"]` (for example on `find` when `ctx.provider` is `"websocket"`).

Publish from `after_commit` with `on: :mutate`. Use `publish_json` for the `{event, path, result}` object. Publish only. Do not also call `send_topic` in that rule if the same process subscribes, or local sockets can get the payload twice. `publish` and `publish_json` are buffered: call `flush` when the process must send now.

If `publish` or `flush` returns `Alumna::Nats::Error`, the example returns `nil` so the client still sees the write. You may return `ServiceError` instead. The write already happened.

The subscribe handler must not raise. `send_topic` swallows `IO::Error` on one socket so others still get the payload. An uncaught raise in the handler goes to the NATS driver `on_error` (default no-op).

See `examples/websocket_fanout.cr`.

---

## 11. License

MIT
