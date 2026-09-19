require "../spec_helper"

describe "Alumna::Nats JetStream consumer" do
  it "creates a durable push consumer, push-subscribes, and acks" do
    stream = unique_stream_name("cons")
    name = unique_consumer_name("workers")
    subject = unique_subject("js-cons")
    js = SHARED.jetstream
    incoming = Channel(Alumna::Nats::JetStream::Message).new
    begin
      must_create_stream(js, stream, [subject])
      consumer = must_create_consumer(js, stream, name)
      consumer.stream.should eq(stream)
      consumer.name.should eq(name)
      consumer.deliver_subject.should eq("_ALUMNA.JS.#{stream}.#{name}")
      consumer.deliver_group.should eq(name)
      consumer.filter_subject.should be_nil
      consumer.num_pending.should eq(0)
      consumer.num_ack_pending.should eq(0)

      sub = must_js_subscribe(js, consumer) { |msg| incoming.send(msg) }
      sub.should be_a(Alumna::Nats::Subscription)

      ack = js.publish(subject, "job")
      if ack.is_a?(Alumna::Nats::Error)
        fail ack.message
      end
      SHARED.flush.should be_nil

      msg = wait_nats(incoming)
      msg.subject.should eq(subject)
      String.new(msg.body).should eq("job")
      msg.stream.should eq(stream)
      msg.consumer.should eq(name)
      msg.delivered_count.should eq(1)
      msg.reply_to.empty?.should be_false

      js.ack(msg).should be_nil
      SHARED.flush.should be_nil

      info = js.consumer_info(stream, name)
      if info.is_a?(Alumna::Nats::Error)
        fail info.message
      end
      if info
        info.num_ack_pending.should eq(0)
      else
        fail "expected consumer info"
      end

      js.unsubscribe(sub).should be_nil
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "does not ack when the handler returns" do
    stream = unique_stream_name("noack")
    name = unique_consumer_name("noack")
    subject = unique_subject("js-noack")
    js = SHARED.jetstream
    incoming = Channel(Alumna::Nats::JetStream::Message).new
    begin
      must_create_stream(js, stream, [subject])
      consumer = must_create_consumer(js, stream, name)
      sub = must_js_subscribe(js, consumer) { |msg| incoming.send(msg) }

      js.publish(subject, "held")
      SHARED.flush
      wait_nats(incoming)

      info = js.consumer_info(stream, name)
      if info.is_a?(Alumna::Nats::Error)
        fail info.message
      end
      if info
        info.num_ack_pending.should eq(1)
      else
        fail "expected consumer info"
      end

      js.unsubscribe(sub)
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "redelivers after nack" do
    stream = unique_stream_name("nack")
    name = unique_consumer_name("nack")
    subject = unique_subject("js-nack")
    js = SHARED.jetstream
    incoming = Channel(Alumna::Nats::JetStream::Message).new
    begin
      must_create_stream(js, stream, [subject])
      consumer = must_create_consumer(js, stream, name)
      sub = must_js_subscribe(js, consumer) { |msg| incoming.send(msg) }

      js.publish(subject, "again")
      SHARED.flush

      first = wait_nats(incoming)
      String.new(first.body).should eq("again")
      first.delivered_count.should eq(1)
      js.nack(first).should be_nil
      SHARED.flush

      second = wait_nats(incoming)
      String.new(second.body).should eq("again")
      second.delivered_count.should eq(2)
      js.ack(second)
      SHARED.flush
      js.unsubscribe(sub)
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "waits before redelivery when nack has a delay" do
    stream = unique_stream_name("delay")
    name = unique_consumer_name("delay")
    subject = unique_subject("js-delay")
    js = SHARED.jetstream
    incoming = Channel(Alumna::Nats::JetStream::Message).new
    begin
      must_create_stream(js, stream, [subject])
      consumer = must_create_consumer(js, stream, name)
      sub = must_js_subscribe(js, consumer) { |msg| incoming.send(msg) }

      js.publish(subject, "later")
      SHARED.flush
      first = wait_nats(incoming)
      js.nack(first, delay: 250.milliseconds).should be_nil
      SHARED.flush

      expect_no_nats(incoming, 80.milliseconds)
      second = wait_nats(incoming, 2.seconds)
      String.new(second.body).should eq("later")
      second.delivered_count.should eq(2)
      js.ack(second)
      SHARED.flush
      js.unsubscribe(sub)
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "delivers each message once when two subscribers share a durable consumer" do
    stream = unique_stream_name("comp")
    name = unique_consumer_name("comp")
    subject = unique_subject("js-comp")
    js = SHARED.jetstream
    incoming = Channel(String).new(32)
    count = 6
    begin
      must_create_stream(js, stream, [subject])
      consumer = must_create_consumer(js, stream, name)
      sub_a = must_js_subscribe(js, consumer) do |msg|
        incoming.send(String.new(msg.body))
        js.ack(msg)
      end
      sub_b = must_js_subscribe(js, consumer) do |msg|
        incoming.send(String.new(msg.body))
        js.ack(msg)
      end

      count.times { |i| js.publish(subject, "m#{i}") }
      SHARED.flush

      received = Array.new(count) { wait_nats(incoming) }
      received.size.should eq(count)
      received.uniq.size.should eq(count)
      expect_no_nats(incoming)
      js.unsubscribe(sub_a)
      js.unsubscribe(sub_b)
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "stores a Bytes payload and a filter subject" do
    stream = unique_stream_name("filt")
    name = unique_consumer_name("filt")
    kept = unique_subject("js-kept")
    skipped = unique_subject("js-skip")
    js = SHARED.jetstream
    incoming = Channel(Bytes).new
    begin
      must_create_stream(js, stream, [kept, skipped])
      consumer = must_create_consumer(js, stream, name, filter_subject: kept)
      consumer.filter_subject.should eq(kept)
      sub = must_js_subscribe(js, consumer) { |msg| incoming.send(msg.body.dup) }

      js.publish(skipped, "no")
      js.publish(kept, "bin".to_slice)
      SHARED.flush

      wait_nats(incoming).should eq("bin".to_slice)
      expect_no_nats(incoming)
      js.unsubscribe(sub)
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "uses an explicit deliver subject and deliver group" do
    stream = unique_stream_name("del")
    name = unique_consumer_name("del")
    subject = unique_subject("js-del")
    deliver = unique_subject("js-inbox")
    group = unique_consumer_name("grp")
    js = SHARED.jetstream
    begin
      must_create_stream(js, stream, [subject])
      consumer = must_create_consumer(js, stream, name, deliver_subject: deliver, deliver_group: group)
      consumer.deliver_subject.should eq(deliver)
      consumer.deliver_group.should eq(group)
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "creates the same durable consumer twice" do
    stream = unique_stream_name("idemp")
    name = unique_consumer_name("idemp")
    subject = unique_subject("js-idemp")
    js = SHARED.jetstream
    begin
      must_create_stream(js, stream, [subject])
      first = must_create_consumer(js, stream, name)
      second = must_create_consumer(js, stream, name)
      first.name.should eq(name)
      second.name.should eq(name)
      second.deliver_subject.should eq(first.deliver_subject)
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "does not create a consumer from subscribe" do
    stream = unique_stream_name("nosub")
    name = unique_consumer_name("nosub")
    subject = unique_subject("js-nosub")
    js = SHARED.jetstream
    incoming = Channel(String).new
    begin
      must_create_stream(js, stream, [subject])
      fabricated = fake_js_consumer(stream: stream, name: name, deliver_subject: unique_subject("ghost"))
      sub = must_js_subscribe(js, fabricated) { |msg| incoming.send(String.new(msg.body)) }
      js.publish(subject, "gone")
      SHARED.flush
      expect_no_nats(incoming)
      info = js.consumer_info(stream, name)
      if info.is_a?(Alumna::Nats::Error)
        fail info.message
      end
      info.should be_nil
      js.unsubscribe(sub)
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "returns nil for consumer_info when the consumer does not exist" do
    stream = unique_stream_name("missing")
    name = unique_consumer_name("missing")
    subject = unique_subject("js-cmiss")
    js = SHARED.jetstream
    begin
      must_create_stream(js, stream, [subject])
      info = js.consumer_info(stream, name)
      if info.is_a?(Alumna::Nats::Error)
        fail info.message
      end
      info.should be_nil
    ensure
      js.delete_stream(stream)
    end
  end

  it "treats delete_consumer of a missing consumer as a no-op" do
    stream = unique_stream_name("gone")
    name = unique_consumer_name("gone")
    subject = unique_subject("js-cgone")
    js = SHARED.jetstream
    begin
      must_create_stream(js, stream, [subject])
      js.delete_consumer(stream, name).should be_nil
    ensure
      js.delete_stream(stream)
    end
  end

  it "returns Error when create_consumer has no stream" do
    result = SHARED.jetstream.create_consumer(unique_stream_name("nostream"), unique_consumer_name("c"))
    result.should be_a(Alumna::Nats::Error)
  end

  it "raises ArgumentError for an empty consumer name" do
    js = SHARED.jetstream
    stream = unique_stream_name("emptyc")
    expect_raises(ArgumentError, "NATS consumer name must not be empty") do
      js.create_consumer(stream, "")
    end
    expect_raises(ArgumentError, "NATS consumer name must not be empty") do
      js.consumer_info(stream, "")
    end
    expect_raises(ArgumentError, "NATS consumer name must not be empty") do
      js.delete_consumer(stream, "")
    end
  end

  it "raises ArgumentError when the consumer name contains a dot" do
    js = SHARED.jetstream
    stream = unique_stream_name("dotc")
    expect_raises(ArgumentError, "NATS consumer name must not contain '.'") do
      js.create_consumer(stream, "bad.name")
    end
    expect_raises(ArgumentError, "NATS consumer name must not contain '.'") do
      js.consumer_info(stream, "bad.name")
    end
    expect_raises(ArgumentError, "NATS consumer name must not contain '.'") do
      js.delete_consumer(stream, "bad.name")
    end
  end

  it "raises ArgumentError for empty stream name on consumer helpers" do
    js = SHARED.jetstream
    name = unique_consumer_name("sempty")
    expect_raises(ArgumentError, "NATS stream name must not be empty") do
      js.create_consumer("", name)
    end
    expect_raises(ArgumentError, "NATS stream name must not be empty") do
      js.consumer_info("", name)
    end
    expect_raises(ArgumentError, "NATS stream name must not be empty") do
      js.delete_consumer("", name)
    end
  end

  it "raises ArgumentError for empty deliver subject, deliver group, or filter subject" do
    js = SHARED.jetstream
    stream = unique_stream_name("opt")
    name = unique_consumer_name("opt")
    expect_raises(ArgumentError, "NATS deliver subject must not be empty") do
      js.create_consumer(stream, name, deliver_subject: "")
    end
    expect_raises(ArgumentError, "NATS deliver group must not be empty") do
      js.create_consumer(stream, name, deliver_group: "")
    end
    expect_raises(ArgumentError, "NATS filter subject must not be empty") do
      js.create_consumer(stream, name, filter_subject: "")
    end
  end

  it "raises ArgumentError when subscribe is not a push consumer" do
    js = SHARED.jetstream
    expect_raises(ArgumentError, "NATS consumer must be a push consumer") do
      js.subscribe(fake_js_consumer(deliver_subject: nil)) { }
    end
    expect_raises(ArgumentError, "NATS consumer must be a push consumer") do
      js.subscribe(fake_js_consumer(deliver_subject: "")) { }
    end
  end

  it "raises ArgumentError for an empty JetStream ack subject" do
    js = SHARED.jetstream
    msg = fake_js_message(reply_to: "")
    expect_raises(ArgumentError, "NATS JetStream ack subject must not be empty") do
      js.ack(msg)
    end
    expect_raises(ArgumentError, "NATS JetStream ack subject must not be empty") do
      js.nack(msg)
    end
  end

  it "raises ArgumentError when nack delay is not greater than zero" do
    js = SHARED.jetstream
    msg = fake_js_message
    expect_raises(ArgumentError, "NATS nack delay must be greater than zero") do
      js.nack(msg, delay: Time::Span.zero)
    end
    expect_raises(ArgumentError, "NATS nack delay must be greater than zero") do
      js.nack(msg, delay: -1.second)
    end
  end

  it "raises ArgumentError when ack wait is not greater than zero" do
    js = SHARED.jetstream
    stream = unique_stream_name("ackwait")
    name = unique_consumer_name("ackwait")
    expect_raises(ArgumentError, "NATS ack wait must be greater than zero") do
      js.create_consumer(stream, name, ack_wait: Time::Span.zero)
    end
    expect_raises(ArgumentError, "NATS ack wait must be greater than zero") do
      js.create_consumer(stream, name, ack_wait: -1.second)
    end
  end

  it "returns Error for consumer helpers after close" do
    holder = connect_nats
    stream = unique_stream_name("closedc")
    name = unique_consumer_name("closedc")
    subject = unique_subject("js-closedc")
    js = holder.jetstream
    begin
      must_create_stream(js, stream, [subject])
      consumer = must_create_consumer(js, stream, name)
      sub = must_js_subscribe(js, consumer) { }
      holder.close.should be_nil
      js.create_consumer(stream, name).should be_a(Alumna::Nats::Error)
      js.consumer_info(stream, name).should be_a(Alumna::Nats::Error)
      js.delete_consumer(stream, name).should be_a(Alumna::Nats::Error)
      js.subscribe(consumer) { }.should be_a(Alumna::Nats::Error)
      js.unsubscribe(sub).should be_a(Alumna::Nats::Error)
      js.ack(fake_js_message).should be_a(Alumna::Nats::Error)
      js.nack(fake_js_message).should be_a(Alumna::Nats::Error)
    ensure
      SHARED.jetstream.delete_consumer(stream, name)
      SHARED.jetstream.delete_stream(stream)
    end
  end
end
