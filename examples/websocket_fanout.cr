# NATS → local Connections glue. Composition in the application.
# Needs Alumna Backend 0.9.1+ and a NATS server (NATS_URL or nats://127.0.0.1:4222).
#
#   crystal run examples/websocket_fanout.cr
#   crystal run examples/websocket_fanout.cr -- --check
#
# after_commit publishes. This process subscribes (no queue group) and calls
# send_topic locally. A WebSocket find watches the service topic.
# Push JSON has event, path, and result. It has no reply id.

require "http/client"
require "http/web_socket"
require "socket"
require "alumna"
require "../src/alumna-nats"

url = ENV["NATS_URL"]? || "nats://127.0.0.1:4222"
nats = Alumna::Nats.new(url)
if nats.is_a?(Alumna::Nats::Error)
  abort "NATS connect failed. Set NATS_URL or start NATS on port 4222. #{nats.message}"
end

at_exit { nats.close }

check = ARGV.includes?("--check")
port = check ? 34771 : 3000

MessageSchema = Alumna::Schema.new
  .str("body", min_length: 1, max_length: 500)
  .str("author", min_length: 1)

app = Alumna::App.new

# Live fan-out between processes. No queue group: every process with sockets
# gets a copy, including the writer. send_topic is an exact local string.
sub = nats.subscribe("messages.>") do |msg|
  app.connections.send_topic("messages", msg.payload)
end
if sub.is_a?(Alumna::Nats::Error)
  abort "subscribe failed: #{sub.message}"
end

app.use "/messages", Alumna.memory(MessageSchema) {
  before validate, on: :write
}

# watch is not a WebSocket frame. A find on this service joins the topic.
app.after on: :find do |ctx|
  if ctx.provider == "websocket"
    if id = ctx.store["connection_id"]?.as?(String)
      ctx.app.connections.watch(id, "messages") unless id.empty?
    end
  end
  nil
end

# Publish only. Do not send_topic here or local sockets can get the payload twice.
app.after_commit on: :mutate do |ctx|
  event = case ctx.method
          when .create? then "created"
          when .update? then "updated"
          when .patch?  then "patched"
          when .remove? then "removed"
          else
            next nil
          end

  raw = ctx.result
  result = case raw
           in Hash(String, Alumna::AnyData)
             raw
           in Array(Hash(String, Alumna::AnyData))
             rows = Array(Alumna::AnyData).new(raw.size)
             raw.each { |row| rows << row }
             rows
           in Nil
             nil
           end

  payload = {} of String => Alumna::AnyData
  payload["event"] = event
  payload["path"] = ctx.path
  payload["result"] = result

  # Ignore bus Error so the client still sees the write.
  nats.publish_json("messages.#{event}", payload)
  nats.flush
  nil
end

def wait_for_port(host : String, port : Int32, timeout : Time::Span = 5.seconds) : Nil
  deadline = Time.instant + timeout
  loop do
    begin
      TCPSocket.new(host, port).close
      return
    rescue
      abort "Server did not start within #{timeout}" if Time.instant > deadline
      Fiber.yield
    end
  end
end

if check
  spawn { app.listen(port, trap_signals: false) }
  wait_for_port("127.0.0.1", port)

  ws = HTTP::WebSocket.new("127.0.0.1", "/", port)
  ws.send(%({"id":"1","method":"find","path":"/messages"}))
  find_raw = ws.receive
  find_body = Alumna::JsonHelper.from_string(find_raw.as(String))
  unless find_body.is_a?(Hash(String, Alumna::AnyData)) && find_body["id"]? == "1"
    abort "find reply missing: #{find_raw}"
  end

  create = HTTP::Client.post(
    "http://127.0.0.1:#{port}/messages",
    headers: HTTP::Headers{"Content-Type" => "application/json"},
    body: %({"body":"Hi","author":"Ada"}),
  )
  unless create.status.created?
    abort "create failed: #{create.status} #{create.body}"
  end

  push_box = Channel(String).new
  spawn { push_box.send(ws.receive.as(String)) }
  push_raw = select
  when value = push_box.receive
    value
  when timeout(2.seconds)
    abort "timeout waiting for WebSocket push"
  end

  push = Alumna::JsonHelper.from_string(push_raw)
  unless push.is_a?(Hash(String, Alumna::AnyData))
    abort "push is not an object: #{push_raw}"
  end
  if push.has_key?("id")
    abort "push must not use a reply id: #{push_raw}"
  end
  unless push["event"]? == "created" && push["path"]? == "/messages"
    abort "unexpected push: #{push_raw}"
  end

  puts "push: #{push_raw}"
  ws.close
  app.close
else
  app.listen(port)
end
