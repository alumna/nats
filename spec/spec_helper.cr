require "spec"
require "uuid"
require "../src/alumna-nats"

# Specs need a NATS server. NATS_URL or local 4222.
NATS_URL = ENV["NATS_URL"]? || "nats://127.0.0.1:4222"

# Unique token for this process so leftover subjects do not collide later.
SPEC_PREFIX = "alumna-spec.#{UUID.random}"

DEAD_HOST = "127.0.0.1"
DEAD_PORT = 42220

def display_nats_url(url : String) : String
  uri = URI.parse(url)
  uri.user = nil
  uri.password = nil
  uri.to_s
rescue
  url.gsub(/\/\/[^\/\s]*@/, "//")
end

def dead_url : String
  "nats://user:secret@#{DEAD_HOST}:#{DEAD_PORT}"
end

def connect_nats(servers : URI | String | Array(URI) | Array(String) = NATS_URL, **opts) : Alumna::Nats
  result = Alumna::Nats.new(servers, **opts)
  if result.is_a?(Alumna::Nats::Error)
    abort "NATS connect failed at #{display_nats_url(NATS_URL)}. #{result.message}"
  end
  result
end

def probe_nats : Nil
  result = Alumna::Nats.new(NATS_URL)
  if result.is_a?(Alumna::Nats::Error)
    abort "NATS is not available at #{display_nats_url(NATS_URL)}. Set NATS_URL or start NATS on port 4222. #{result.message}"
  end
  ping = result.ping
  if ping.is_a?(Alumna::Nats::Error)
    abort "NATS is not available at #{display_nats_url(NATS_URL)}. Set NATS_URL or start NATS on port 4222. #{ping.message}"
  end
  result.close
rescue ex
  safe = Alumna::Nats::Errors.safe_message(ex)
  abort "NATS is not available at #{display_nats_url(NATS_URL)}. Set NATS_URL or start NATS on port 4222. #{safe}"
end

probe_nats

SHARED = connect_nats

def unique_subject(label : String = "s") : String
  "#{SPEC_PREFIX}.#{label}.#{UUID.random}"
end

def wait_nats(channel : Channel(T), timeout : Time::Span = 2.seconds) : T forall T
  select
  when value = channel.receive
    value
  when timeout(timeout)
    fail "timeout waiting for NATS message"
  end
end

def expect_no_nats(channel : Channel(T), wait : Time::Span = 200.milliseconds) : Nil forall T
  select
  when value = channel.receive
    fail "unexpected NATS message: #{value.inspect}"
  when timeout(wait)
  end
end

def must_subscribe(holder : Alumna::Nats, subject : String, *, queue_group : String? = nil, &block : Alumna::Nats::Message ->) : Alumna::Nats::Subscription
  result = holder.subscribe(subject, queue_group: queue_group, &block)
  if result.is_a?(Alumna::Nats::Error)
    fail result.message
  end
  result
end

def unique_queue_group(label : String = "qg") : String
  "#{SPEC_PREFIX}.#{label}.#{UUID.random}"
end

# Stream names must not contain '.'.
def unique_stream_name(label : String = "st") : String
  "alumna-#{label}-#{UUID.random}"
end

def must_create_stream(
  js : Alumna::Nats::JetStream,
  name : String,
  subjects : Array(String),
  *,
  storage : Alumna::Nats::JetStream::Storage = :memory,
  retention : Alumna::Nats::JetStream::Retention = :limits,
) : Alumna::Nats::JetStream::Stream
  result = js.create_stream(name, subjects, storage: storage, retention: retention)
  if result.is_a?(Alumna::Nats::Error)
    fail result.message
  end
  result
end

# Consumer names must not contain '.'.
def unique_consumer_name(label : String = "c") : String
  "alumna-#{label}-#{UUID.random}"
end

def must_create_consumer(
  js : Alumna::Nats::JetStream,
  stream : String,
  name : String,
  **opts,
) : Alumna::Nats::JetStream::Consumer
  result = js.create_consumer(stream, name, **opts)
  if result.is_a?(Alumna::Nats::Error)
    fail result.message
  end
  result
end

def must_js_subscribe(
  js : Alumna::Nats::JetStream,
  consumer : Alumna::Nats::JetStream::Consumer,
  &block : Alumna::Nats::JetStream::Message ->
) : Alumna::Nats::Subscription
  result = js.subscribe(consumer, &block)
  if result.is_a?(Alumna::Nats::Error)
    fail result.message
  end
  result
end

def fake_js_consumer(
  *,
  stream : String = unique_stream_name("fake"),
  name : String = unique_consumer_name("fake"),
  deliver_subject : String? = unique_subject("del"),
  deliver_group : String? = "g",
  filter_subject : String? = nil,
) : Alumna::Nats::JetStream::Consumer
  Alumna::Nats::JetStream::Consumer.new(stream, name, deliver_subject, deliver_group, filter_subject, 0_i64, 0_i64)
end

def fake_js_message(*, reply_to : String = "x") : Alumna::Nats::JetStream::Message
  Alumna::Nats::JetStream::Message.new("s", "", "st", "c", 1_i64, reply_to)
end

def wait_stream_messages(
  js : Alumna::Nats::JetStream,
  name : String,
  count : Int64,
  timeout : Time::Span = 2.seconds,
) : Alumna::Nats::JetStream::Stream
  start = Time.instant
  loop do
    info = js.stream_info(name)
    if info.is_a?(Alumna::Nats::Error)
      fail info.message
    end
    if info && info.messages == count
      return info
    end
    if start.elapsed >= timeout
      seen = info ? info.messages : "missing"
      fail "timeout waiting for stream #{name} messages == #{count} (was #{seen})"
    end
    sleep 20.milliseconds
  end
end
