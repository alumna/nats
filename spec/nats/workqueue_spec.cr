require "../spec_helper"

describe "Alumna::Nats JetStream workqueue" do
  it "discards the message after the first ack" do
    stream = unique_stream_name("wqack")
    name = unique_consumer_name("wqack")
    subject = unique_subject("js-wqack")
    js = SHARED.jetstream
    incoming = Channel(Alumna::Nats::JetStream::Message).new
    begin
      created = must_create_stream(js, stream, [subject], retention: :workqueue)
      created.retention.should eq(Alumna::Nats::JetStream::Retention::Workqueue)
      consumer = must_create_consumer(js, stream, name)
      sub = must_js_subscribe(js, consumer) { |msg| incoming.send(msg) }

      js.publish(subject, "job")
      SHARED.flush
      msg = wait_nats(incoming)
      String.new(msg.body).should eq("job")

      before = js.stream_info(stream)
      if before.is_a?(Alumna::Nats::Error)
        fail before.message
      end
      if before
        before.messages.should eq(1)
      else
        fail "expected stream info"
      end

      js.ack(msg).should be_nil
      SHARED.flush
      wait_stream_messages(js, stream, 0)

      js.unsubscribe(sub)
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "returns Error when a second consumer shares workqueue interest" do
    stream = unique_stream_name("wqtwo")
    first = unique_consumer_name("wqone")
    second = unique_consumer_name("wqtwo")
    subject = unique_subject("js-wqtwo")
    js = SHARED.jetstream
    begin
      must_create_stream(js, stream, [subject], retention: :workqueue)
      must_create_consumer(js, stream, first)
      result = js.create_consumer(stream, second)
      result.should be_a(Alumna::Nats::Error)
    ensure
      js.delete_consumer(stream, first)
      js.delete_consumer(stream, second)
      js.delete_stream(stream)
    end
  end

  it "delivers each job once when two workers share a workqueue consumer" do
    stream = unique_stream_name("wqjob")
    name = unique_consumer_name("workers")
    subject = unique_subject("js-wqjob")
    js = SHARED.jetstream
    incoming = Channel(String).new(32)
    count = 6
    begin
      must_create_stream(js, stream, [subject], retention: :workqueue)
      consumer = must_create_consumer(js, stream, name)
      sub_a = must_js_subscribe(js, consumer) do |msg|
        incoming.send(String.new(msg.body))
        js.ack(msg)
      end
      sub_b = must_js_subscribe(js, consumer) do |msg|
        incoming.send(String.new(msg.body))
        js.ack(msg)
      end

      count.times { |i| js.publish(subject, "j#{i}") }
      SHARED.flush

      received = Array.new(count) { wait_nats(incoming) }
      received.size.should eq(count)
      received.uniq.size.should eq(count)
      expect_no_nats(incoming)
      SHARED.flush
      wait_stream_messages(js, stream, 0)

      js.unsubscribe(sub_a)
      js.unsubscribe(sub_b)
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end

  it "keeps the message after ack when retention is limits" do
    stream = unique_stream_name("limack")
    name = unique_consumer_name("limack")
    subject = unique_subject("js-limack")
    js = SHARED.jetstream
    incoming = Channel(Alumna::Nats::JetStream::Message).new
    begin
      must_create_stream(js, stream, [subject], retention: :limits)
      consumer = must_create_consumer(js, stream, name)
      sub = must_js_subscribe(js, consumer) { |msg| incoming.send(msg) }

      js.publish(subject, "keep")
      SHARED.flush
      msg = wait_nats(incoming)
      js.ack(msg)
      SHARED.flush
      wait_stream_messages(js, stream, 1)

      js.unsubscribe(sub)
    ensure
      js.delete_consumer(stream, name)
      js.delete_stream(stream)
    end
  end
end
