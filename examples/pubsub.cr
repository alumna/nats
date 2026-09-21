# Core NATS pub/sub. Every current subscriber gets a copy. No store.
# Needs a NATS server (NATS_URL or nats://127.0.0.1:4222).
#
#   crystal run examples/pubsub.cr

require "../src/alumna-nats"

url = ENV["NATS_URL"]? || "nats://127.0.0.1:4222"
nats = Alumna::Nats.new(url)
if nats.is_a?(Alumna::Nats::Error)
  abort "NATS connect failed. Set NATS_URL or start NATS on port 4222. #{nats.message}"
end

at_exit { nats.close }

got_a = Channel(String).new
got_b = Channel(String).new

sub_a = nats.subscribe("orders.created") { |msg| got_a.send(String.new(msg.body)) }
if sub_a.is_a?(Alumna::Nats::Error)
  abort "subscribe failed: #{sub_a.message}"
end

sub_b = nats.subscribe("orders.created") { |msg| got_b.send(String.new(msg.body)) }
if sub_b.is_a?(Alumna::Nats::Error)
  abort "subscribe failed: #{sub_b.message}"
end

# No queue_group: both subscribers get a copy (live fan-out).
pub = nats.publish("orders.created", %({"id":1}))
if pub.is_a?(Alumna::Nats::Error)
  abort "publish failed: #{pub.message}"
end
flush = nats.flush
if flush.is_a?(Alumna::Nats::Error)
  abort "flush failed: #{flush.message}"
end

def wait_copy(ch : Channel(String), who : String) : String
  select
  when body = ch.receive
    body
  when timeout(2.seconds)
    abort "timeout waiting for #{who}"
  end
end

a = wait_copy(got_a, "subscriber A")
b = wait_copy(got_b, "subscriber B")
puts "subscriber A: #{a}"
puts "subscriber B: #{b}"

nats.unsubscribe(sub_a)
nats.unsubscribe(sub_b)
