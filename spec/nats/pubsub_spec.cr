require "../spec_helper"

describe "Alumna::Nats core pub/sub" do
  it "publishes a String and the subscriber receives it without blocking subscribe" do
    subject = unique_subject("str")
    incoming = Channel(Alumna::Nats::Message).new

    sub = must_subscribe(SHARED, subject) { |msg| incoming.send(msg) }
    sub.should be_a(Alumna::Nats::Subscription)
    sub.subject.should eq(subject)

    SHARED.publish(subject, "hello").should be_nil
    SHARED.flush.should be_nil

    msg = wait_nats(incoming)
    msg.subject.should eq(subject)
    String.new(msg.body).should eq("hello")

    SHARED.unsubscribe(sub).should be_nil
  end

  it "publishes Bytes and the subscriber receives the same bytes" do
    subject = unique_subject("bytes")
    incoming = Channel(Bytes).new

    sub = must_subscribe(SHARED, subject) { |msg| incoming.send(msg.body.dup) }
    SHARED.publish(subject, "bin-ok".to_slice).should be_nil
    SHARED.flush

    wait_nats(incoming).should eq("bin-ok".to_slice)
    SHARED.unsubscribe(sub)
  end

  it "delivers a copy to every current subscriber" do
    subject = unique_subject("fan")
    first = Channel(String).new
    second = Channel(String).new

    sub_a = must_subscribe(SHARED, subject) { |msg| first.send(String.new(msg.body)) }
    sub_b = must_subscribe(SHARED, subject) { |msg| second.send(String.new(msg.body)) }

    SHARED.publish(subject, "copy")
    SHARED.flush

    wait_nats(first).should eq("copy")
    wait_nats(second).should eq("copy")

    SHARED.unsubscribe(sub_a)
    SHARED.unsubscribe(sub_b)
  end

  it "stops delivery after unsubscribe and leaves other subscribers in place" do
    subject = unique_subject("unsub")
    kept = Channel(String).new
    dropped = Channel(String).new

    sub_keep = must_subscribe(SHARED, subject) { |msg| kept.send(String.new(msg.body)) }
    sub_drop = must_subscribe(SHARED, subject) { |msg| dropped.send(String.new(msg.body)) }
    SHARED.unsubscribe(sub_drop).should be_nil
    SHARED.flush

    SHARED.publish(subject, "only-keep")
    SHARED.flush

    wait_nats(kept).should eq("only-keep")
    expect_no_nats(dropped)
    SHARED.unsubscribe(sub_keep)
  end

  it "does not deliver a message published before subscribe" do
    subject = unique_subject("miss")
    incoming = Channel(String).new

    SHARED.publish(subject, "gone")
    SHARED.flush.should be_nil

    sub = must_subscribe(SHARED, subject) { |msg| incoming.send(String.new(msg.body)) }
    SHARED.flush
    expect_no_nats(incoming)
    SHARED.unsubscribe(sub)
  end

  it "matches a subscribe wildcard against a concrete subject" do
    token = UUID.random
    pattern = "#{SPEC_PREFIX}.wild.#{token}.*"
    concrete = "#{SPEC_PREFIX}.wild.#{token}.created"
    incoming = Channel(Alumna::Nats::Message).new

    sub = must_subscribe(SHARED, pattern) { |msg| incoming.send(msg) }
    sub.subject.should eq(pattern)
    SHARED.publish(concrete, "order")
    SHARED.flush

    msg = wait_nats(incoming)
    msg.subject.should eq(concrete)
    String.new(msg.body).should eq("order")
    SHARED.unsubscribe(sub)
  end

  it "raises ArgumentError for an empty subject on publish and subscribe" do
    expect_raises(ArgumentError, "NATS subject must not be empty") do
      SHARED.publish("", "x")
    end
    expect_raises(ArgumentError, "NATS subject must not be empty") do
      SHARED.subscribe("") { }
    end
  end

  it "raises ArgumentError for an invalid publish or subscribe subject" do
    expect_raises(ArgumentError) do
      SHARED.publish("bad subject", "x")
    end
    expect_raises(ArgumentError) do
      SHARED.publish("star*", "x")
    end
    expect_raises(ArgumentError) do
      SHARED.subscribe("bad subject") { }
    end
  end

  it "returns Error when the payload is larger than the server max" do
    subject = unique_subject("oversize")
    oversize = "a" * (SHARED.client.server_info.max_payload + 1)
    result = SHARED.publish(subject, oversize)
    result.should be_a(Alumna::Nats::Error)
  end

  it "returns Error for publish, subscribe, and unsubscribe after close" do
    holder = connect_nats
    subject = unique_subject("closed")
    incoming = Channel(String).new
    sub = must_subscribe(holder, subject) { |msg| incoming.send(String.new(msg.body)) }
    holder.close.should be_nil

    holder.publish(subject, "nope").should be_a(Alumna::Nats::Error)
    holder.subscribe(subject) { }.should be_a(Alumna::Nats::Error)
    holder.unsubscribe(sub).should be_a(Alumna::Nats::Error)
  end
end
