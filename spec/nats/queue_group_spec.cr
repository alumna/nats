require "../spec_helper"

describe "Alumna::Nats core queue group" do
  it "delivers each message once when two subscribers share a queue group" do
    subject = unique_subject("qg-one")
    group = unique_queue_group("workers")
    incoming = Channel(String).new(32)
    count = 10

    sub_a = must_subscribe(SHARED, subject, queue_group: group) { |msg| incoming.send(String.new(msg.body)) }
    sub_b = must_subscribe(SHARED, subject, queue_group: group) { |msg| incoming.send(String.new(msg.body)) }
    sub_a.queue_group.should eq(group)
    sub_b.queue_group.should eq(group)

    count.times { |i| SHARED.publish(subject, "m#{i}") }
    SHARED.flush.should be_nil

    received = Array.new(count) { wait_nats(incoming) }
    received.size.should eq(count)
    received.uniq.size.should eq(count)
    expect_no_nats(incoming)

    SHARED.unsubscribe(sub_a)
    SHARED.unsubscribe(sub_b)
  end

  it "gives a copy to each queue group on the same subject" do
    subject = unique_subject("qg-two")
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

  it "keeps fan-out when subscribe has no queue group" do
    subject = unique_subject("qg-fan")
    first = Channel(String).new
    second = Channel(String).new

    sub_a = must_subscribe(SHARED, subject) { |msg| first.send(String.new(msg.body)) }
    sub_b = must_subscribe(SHARED, subject) { |msg| second.send(String.new(msg.body)) }
    sub_a.queue_group.should be_nil
    sub_b.queue_group.should be_nil

    SHARED.publish(subject, "copy")
    SHARED.flush

    wait_nats(first).should eq("copy")
    wait_nats(second).should eq("copy")

    SHARED.unsubscribe(sub_a)
    SHARED.unsubscribe(sub_b)
  end

  it "delivers to the remaining member after one queue subscriber unsubscribes" do
    subject = unique_subject("qg-unsub")
    group = unique_queue_group("left")
    kept = Channel(String).new
    dropped = Channel(String).new

    sub_keep = must_subscribe(SHARED, subject, queue_group: group) { |msg| kept.send(String.new(msg.body)) }
    sub_drop = must_subscribe(SHARED, subject, queue_group: group) { |msg| dropped.send(String.new(msg.body)) }
    SHARED.unsubscribe(sub_drop).should be_nil
    SHARED.flush

    SHARED.publish(subject, "only-keep")
    SHARED.flush

    wait_nats(kept).should eq("only-keep")
    expect_no_nats(dropped)
    SHARED.unsubscribe(sub_keep)
  end

  it "raises ArgumentError for an empty queue group" do
    expect_raises(ArgumentError, "NATS queue group must not be empty") do
      SHARED.subscribe(unique_subject("qg-empty"), queue_group: "") { }
    end
  end

  it "returns Error for queue-group subscribe after close" do
    holder = connect_nats
    subject = unique_subject("qg-closed")
    group = unique_queue_group("closed")
    holder.close.should be_nil
    holder.subscribe(subject, queue_group: group) { }.should be_a(Alumna::Nats::Error)
  end
end
