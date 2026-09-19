require "../spec_helper"

# Closed delivery-model matrix (live nats-server). Unique names per example.
# Isolation is not a server flush alone. Delete streams and consumers in ensure.
describe "Alumna::Nats delivery models" do
  it "core fan-out: two subscribers both get the message, including one wildcard" do
    token = UUID.random
    concrete = "#{SPEC_PREFIX}.orders.#{token}.created"
    wild = "#{SPEC_PREFIX}.orders.#{token}.*"
    first = Channel(String).new
    second = Channel(String).new

    sub_exact = must_subscribe(SHARED, concrete) { |msg| first.send(String.new(msg.body)) }
    sub_wild = must_subscribe(SHARED, wild) { |msg| second.send(String.new(msg.body)) }

    SHARED.publish(concrete, "fan")
    SHARED.flush

    wait_nats(first).should eq("fan")
    wait_nats(second).should eq("fan")

    SHARED.unsubscribe(sub_exact)
    SHARED.unsubscribe(sub_wild)
  end

  it "core miss: a late subscribe does not get the old message" do
    subject = unique_subject("miss-e2e")
    incoming = Channel(String).new

    SHARED.publish(subject, "gone")
    SHARED.flush.should be_nil

    sub = must_subscribe(SHARED, subject) { |msg| incoming.send(String.new(msg.body)) }
    SHARED.flush
    expect_no_nats(incoming)
    SHARED.unsubscribe(sub)
  end

  it "core queue group: two workers, each message once, total N" do
    subject = unique_subject("qg-e2e")
    group = unique_queue_group("workers")
    incoming = Channel(String).new(32)
    count = 10

    sub_a = must_subscribe(SHARED, subject, queue_group: group) { |msg| incoming.send(String.new(msg.body)) }
    sub_b = must_subscribe(SHARED, subject, queue_group: group) { |msg| incoming.send(String.new(msg.body)) }

    count.times { |i| SHARED.publish(subject, "m#{i}") }
    SHARED.flush.should be_nil

    received = Array.new(count) { wait_nats(incoming) }
    received.size.should eq(count)
    received.uniq.size.should eq(count)
    expect_no_nats(incoming)

    SHARED.unsubscribe(sub_a)
    SHARED.unsubscribe(sub_b)
  end

  it "two queue groups: each group gets a copy" do
    subject = unique_subject("twoqg-e2e")
    group_a = unique_queue_group("ga")
    group_b = unique_queue_group("gb")
    first = Channel(String).new
    second = Channel(String).new

    sub_a = must_subscribe(SHARED, subject, queue_group: group_a) { |msg| first.send(String.new(msg.body)) }
    sub_b = must_subscribe(SHARED, subject, queue_group: group_b) { |msg| second.send(String.new(msg.body)) }

    SHARED.publish(subject, "both")
    SHARED.flush

    wait_nats(first).should eq("both")
    wait_nats(second).should eq("both")

    SHARED.unsubscribe(sub_a)
    SHARED.unsubscribe(sub_b)
  end

  it "JS workqueue: worker does not ack, the other worker gets the redelivery" do
    stream = unique_stream_name("wqredel")
    name = unique_consumer_name("wqredel")
    subject = unique_subject("js-wqredel")
    js = SHARED.jetstream
    first = Channel(Alumna::Nats::JetStream::Message).new
    second = Channel(Alumna::Nats::JetStream::Message).new
    begin
      must_create_stream(js, stream, [subject], retention: :workqueue)
      consumer = must_create_consumer(js, stream, name, ack_wait: 250.milliseconds)
      sub_a = must_js_subscribe(js, consumer) { |msg| first.send(msg) }
      sub_b = must_js_subscribe(js, consumer) { |msg| second.send(msg) }

      ack = js.publish(subject, "job")
      if ack.is_a?(Alumna::Nats::Error)
        fail ack.message
      end
      SHARED.flush

      remaining_ch = nil.as(Channel(Alumna::Nats::JetStream::Message)?)
      remaining_sub = nil.as(Alumna::Nats::Subscription?)
      select
      when msg = first.receive
        String.new(msg.body).should eq("job")
        msg.delivered_count.should eq(1)
        js.unsubscribe(sub_a).should be_nil
        remaining_ch = second
        remaining_sub = sub_b
      when msg = second.receive
        String.new(msg.body).should eq("job")
        msg.delivered_count.should eq(1)
        js.unsubscribe(sub_b).should be_nil
        remaining_ch = first
        remaining_sub = sub_a
      when timeout(2.seconds)
        fail "timeout waiting for first workqueue delivery"
      end
      SHARED.flush

      if remaining_ch && remaining_sub
        redelivered = wait_nats(remaining_ch, 3.seconds)
        String.new(redelivered.body).should eq("job")
        redelivered.delivered_count.should be >= 2
        js.ack(redelivered).should be_nil
        SHARED.flush
        wait_stream_messages(js, stream, 0)
        js.unsubscribe(remaining_sub)
      else
        fail "expected a remaining workqueue worker"
      end
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "JS durable fan-out: independent limits consumers, late deliver-all receives history" do
    stream = unique_stream_name("dfan")
    first_name = unique_consumer_name("dfana")
    second_name = unique_consumer_name("dfanb")
    subject = unique_subject("js-dfan")
    js = SHARED.jetstream
    first = Channel(String).new
    second = Channel(String).new
    begin
      created = must_create_stream(js, stream, [subject], retention: :limits)
      created.retention.should eq(Alumna::Nats::JetStream::Retention::Limits)

      ack = js.publish(subject, "history")
      if ack.is_a?(Alumna::Nats::Error)
        fail ack.message
      end
      SHARED.flush

      consumer_a = must_create_consumer(js, stream, first_name)
      consumer_b = must_create_consumer(js, stream, second_name)
      sub_a = must_js_subscribe(js, consumer_a) do |msg|
        first.send(String.new(msg.body))
        js.ack(msg)
      end
      sub_b = must_js_subscribe(js, consumer_b) do |msg|
        second.send(String.new(msg.body))
        js.ack(msg)
      end
      SHARED.flush

      wait_nats(first).should eq("history")
      wait_nats(second).should eq("history")

      js.unsubscribe(sub_a)
      js.unsubscribe(sub_b)
    ensure
      js.delete_consumer(stream, first_name)
      js.delete_consumer(stream, second_name)
      js.delete_stream(stream)
    end
  end

  it "JS no stream: publish returns Error and the process stays up" do
    subject = unique_subject("js-nostream-e2e")
    js = SHARED.jetstream

    result = js.publish(subject, "x")
    result.should be_a(Alumna::Nats::Error)

    SHARED.ping.should be_nil
    SHARED.flush.should be_nil
  end
end
