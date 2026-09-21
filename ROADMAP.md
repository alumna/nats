# Alumna NATS — roadmap

Official NATS pub/sub and JetStream jobs for Alumna Backend. Not a Service adapter. No `AdapterSuite`. No HTTP WebSocket in this shard.

Driver: `jgaskins/nats`. One `NATS::Client` per process. URI, URL string, or a list of servers. No Unix socket.

## Delivered

* `Alumna::Nats` connection holder: `new` / `from_uri` / `from_env`, `ping`, `flush`, `close`.
* Error helper strips URI userinfo. Operation errors are the struct `Alumna::Nats::Error`. Config errors raise `ArgumentError`.
* Core publish and subscribe. Payload `String` or `Bytes`. App owns subjects. `subscribe` does not block.
* Core queue group on subscribe. Competing consumers. No persist.
* JetStream stream helper: create, info, delete. Publish does not create a stream.
* JetStream durable push consumer, subscribe, explicit ack and nack. The handler does not ack.
* JetStream workqueue retention. The first ack removes the message. This is the job queue.
* Closed delivery-model matrix on a live NATS server: core fan-out, miss, queue groups, JetStream workqueue redelivery, JetStream durable fan-out, and publish with no stream.
* README: install, connect, pub/sub, queue group, JetStream jobs, errors, security, testing, examples, and WebSocket fan-out composition.
* GitHub CI: NATS 2.10 with JetStream, format, spec, `preview_mt` + `execution_context`, kcov 100% on `src/`.
* Examples: core pub/sub, queue group and JetStream jobs, WebSocket fan-out with local Connections. The shard `src/` does not import HTTP WebSocket.

## Next

Request/reply, KV, Object store, and NATS Services API are out of this product.
