# Changelog

## Unreleased

### Added
* `Message#payload` and `JetStream::Message#payload` return the server text. `body` is a byte view of that text.
* `publish_json` encodes `AnyData` as JSON.

### Changed
* `jetstream` returns the same helper for one client.
* `nack` with `delay` writes the payload into a stack buffer.
* `Errors.safe_message` returns the original message when it has no URI userinfo.

## v0.1.1

### Changed
* Requires Alumna Backend `>= 0.10.1`.

## v0.1.0

### Added
* Requires Alumna Backend `~> 0.9.1`.
* Examples: core pub/sub, queue group and JetStream jobs, and WebSocket fan-out glue (`examples/`).
* Driver `jgaskins/nats` 1.7.0.
* `Alumna::Nats` connection holder. `new` (URI, URL string, or server list), `from_uri`, and `from_env`. Pass `nkeys_file` and `user_credentials`. `ping`, `flush`, and `close`. One `NATS::Client` per process.
* `Alumna::Nats::Error` struct and `Errors.safe_message` / `Errors.wrap`. Messages never include URI userinfo. `ArgumentError` for a bad URL, a missing environment variable, an empty or invalid subject, or an empty queue group.
* Core `publish`, `subscribe`, and `unsubscribe`. Payload is `String` or `Bytes`. `subscribe` does not block. Returns `Alumna::Nats::Subscription` or `Alumna::Nats::Error`. `Alumna::Nats::Message` has `subject` and `body` (`Bytes`).
* Core queue group on `subscribe` (`queue_group:`). Competing consumers. At-most-once. No persist. Empty queue group raises `ArgumentError`.
* JetStream stream helper: `create_stream`, `stream_info`, `delete_stream`. `jetstream.publish` does not create a stream. If no stream listens, publish returns `Alumna::Nats::Error`. Empty stream name, a name with `.`, or empty subjects raise `ArgumentError`.
* JetStream durable push consumer: `create_consumer`, `consumer_info`, `delete_consumer`, `subscribe`, `unsubscribe`, `ack`, and `nack`. Optional `delay:` on `nack`. Subscribe does not create a consumer. The handler does not ack. Pull consumers are not in this product. Empty consumer name or a name with `.` raises `ArgumentError`.
* JetStream workqueue retention is the job queue. The first ack removes the message. A second consumer on the same interest returns `Alumna::Nats::Error`. Workers compete on one durable consumer.
* Optional `ack_wait` on `create_consumer`. Deliver policy is all: a late consumer receives stored messages.
* Specs for the closed delivery-model matrix: core fan-out (with one wildcard), core miss, core queue group, two queue groups, JetStream workqueue redelivery, JetStream durable fan-out on limits, and JetStream publish with no stream.
* README: install, connect, pub/sub, queue group, JetStream jobs, errors, security, testing, examples, and WebSocket fan-out.
* GitHub CI: NATS 2.10 service with JetStream, `format --check`, `crystal spec`, `preview_mt` + `execution_context`, kcov 100% on `src/`.

### Changed
* **docs:** README documents examples and WebSocket fan-out composition (subscribe → local `Connections.send_topic`). Backend does not import NATS. This shard does not import HTTP WebSocket.
* **docs:** README errors section uses tables for return types and `ArgumentError`.
