# Competing consumers, then a durable job queue.
# Needs a NATS server with JetStream (NATS_URL or nats://127.0.0.1:4222).
#
#   crystal run examples/jobs.cr
#
# Core queue_group: one worker in the group gets each message. No store.
# JetStream workqueue: the job waits. The first ack removes it.

require "../src/alumna-nats"

url = ENV["NATS_URL"]? || "nats://127.0.0.1:4222"
nats = Alumna::Nats.new(url)
if nats.is_a?(Alumna::Nats::Error)
  abort "NATS connect failed. Set NATS_URL or start NATS on port 4222. #{nats.message}"
end

at_exit { nats.close }

def wait_body(ch : Channel(String), label : String) : String
  select
  when body = ch.receive
    body
  when timeout(2.seconds)
    abort "timeout waiting for #{label}"
  end
end

# --- Core queue group (ephemeral) ---

queue_got = Channel(String).new
sub_a = nats.subscribe("example.queue.email", queue_group: "workers") do |msg|
  queue_got.send("A:#{msg.payload}")
end
if sub_a.is_a?(Alumna::Nats::Error)
  abort "queue subscribe A failed: #{sub_a.message}"
end
sub_b = nats.subscribe("example.queue.email", queue_group: "workers") do |msg|
  queue_got.send("B:#{msg.payload}")
end
if sub_b.is_a?(Alumna::Nats::Error)
  abort "queue subscribe B failed: #{sub_b.message}"
end

pub = nats.publish("example.queue.email", "one")
if pub.is_a?(Alumna::Nats::Error)
  abort "queue publish failed: #{pub.message}"
end
nats.flush

first = wait_body(queue_got, "queue group")
puts "queue group delivered once: #{first}"
select
when extra = queue_got.receive
  abort "queue group delivered twice: #{extra}"
when timeout(200.milliseconds)
end

nats.unsubscribe(sub_a)
nats.unsubscribe(sub_b)

# --- JetStream workqueue (job queue) ---

js = nats.jetstream
stream = "examplejobs"
consumer = "workers"
subject = "example.jobs.email"

js.delete_consumer(stream, consumer)
js.delete_stream(stream)

created = js.create_stream(stream, [subject], storage: :memory, retention: :workqueue)
if created.is_a?(Alumna::Nats::Error)
  abort "create_stream failed: #{created.message}"
end

cons = js.create_consumer(stream, consumer, ack_wait: 5.seconds)
if cons.is_a?(Alumna::Nats::Error)
  abort "create_consumer failed: #{cons.message}"
end

job_got = Channel(String).new
sub = js.subscribe(cons) do |msg|
  body = msg.payload
  job_got.send(body)
  js.ack(msg)
end
if sub.is_a?(Alumna::Nats::Error)
  abort "jetstream subscribe failed: #{sub.message}"
end

ack = js.publish(subject, %({"to":"a@example.com"}))
if ack.is_a?(Alumna::Nats::Error)
  abort "jetstream publish failed: #{ack.message}"
end
nats.flush

puts "job: #{wait_body(job_got, "jetstream job")}"

js.unsubscribe(sub)
js.delete_consumer(stream, consumer)
js.delete_stream(stream)
