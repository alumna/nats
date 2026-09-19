require "../spec_helper"

describe Alumna::Nats::JetStream do
  it "creates a stream, returns info, and deletes it" do
    name = unique_stream_name("crud")
    subject = unique_subject("js-crud")
    js = SHARED.jetstream
    begin
      created = must_create_stream(js, name, [subject])
      created.name.should eq(name)
      created.subjects.should eq([subject])
      created.storage.should eq(Alumna::Nats::JetStream::Storage::Memory)
      created.retention.should eq(Alumna::Nats::JetStream::Retention::Limits)
      created.messages.should eq(0)

      info = js.stream_info(name)
      if info.is_a?(Alumna::Nats::Error)
        fail info.message
      end
      if info
        info.name.should eq(name)
        info.subjects.should eq([subject])
        info.storage.should eq(Alumna::Nats::JetStream::Storage::Memory)
        info.messages.should eq(0)
      else
        fail "expected stream info"
      end

      js.delete_stream(name).should be_nil
      missing = js.stream_info(name)
      if missing.is_a?(Alumna::Nats::Error)
        fail missing.message
      end
      missing.should be_nil
    ensure
      js.delete_stream(name)
    end
  end

  it "stores file storage and defaults create_stream storage to file" do
    name = unique_stream_name("file")
    subject = unique_subject("js-file")
    js = SHARED.jetstream
    begin
      created = js.create_stream(name, [subject])
      if created.is_a?(Alumna::Nats::Error)
        fail created.message
      end
      created.storage.should eq(Alumna::Nats::JetStream::Storage::File)

      info = js.stream_info(name)
      if info.is_a?(Alumna::Nats::Error)
        fail info.message
      end
      if info
        info.storage.should eq(Alumna::Nats::JetStream::Storage::File)
      else
        fail "expected stream info"
      end
    ensure
      js.delete_stream(name)
    end
  end

  it "stores limits, interest, and workqueue retention" do
    js = SHARED.jetstream
    {
      Alumna::Nats::JetStream::Retention::Limits,
      Alumna::Nats::JetStream::Retention::Interest,
      Alumna::Nats::JetStream::Retention::Workqueue,
    }.each do |retention|
      name = unique_stream_name("ret")
      subject = unique_subject("js-ret")
      begin
        created = must_create_stream(js, name, [subject], retention: retention)
        created.retention.should eq(retention)
        info = js.stream_info(name)
        if info.is_a?(Alumna::Nats::Error)
          fail info.message
        end
        if info
          info.retention.should eq(retention)
        else
          fail "expected stream info"
        end
      ensure
        js.delete_stream(name)
      end
    end
  end

  it "returns nil for stream_info when the stream does not exist" do
    info = SHARED.jetstream.stream_info(unique_stream_name("missing"))
    if info.is_a?(Alumna::Nats::Error)
      fail info.message
    end
    info.should be_nil
  end

  it "treats delete_stream of a missing stream as a no-op" do
    SHARED.jetstream.delete_stream(unique_stream_name("gone")).should be_nil
  end

  it "returns Error when two streams would share a subject" do
    first = unique_stream_name("a")
    second = unique_stream_name("b")
    subject = unique_subject("js-overlap")
    js = SHARED.jetstream
    begin
      must_create_stream(js, first, [subject])
      js.create_stream(second, [subject], storage: :memory).should be_a(Alumna::Nats::Error)
    ensure
      js.delete_stream(first)
      js.delete_stream(second)
    end
  end

  it "publishes to a created stream and does not create a stream on publish" do
    name = unique_stream_name("pub")
    subject = unique_subject("js-pub")
    js = SHARED.jetstream
    begin
      must_create_stream(js, name, [subject])
      ack = js.publish(subject, "job")
      if ack.is_a?(Alumna::Nats::Error)
        fail ack.message
      end
      ack.stream.should eq(name)
      ack.sequence.should eq(1)
      ack.duplicate.should be_false

      bytes_ack = js.publish(subject, "bin".to_slice)
      if bytes_ack.is_a?(Alumna::Nats::Error)
        fail bytes_ack.message
      end
      bytes_ack.sequence.should eq(2)

      info = js.stream_info(name)
      if info.is_a?(Alumna::Nats::Error)
        fail info.message
      end
      if info
        info.messages.should eq(2)
      else
        fail "expected stream info"
      end
    ensure
      js.delete_stream(name)
    end
  end

  it "returns Error when JetStream publish has no stream and does not create one" do
    subject = unique_subject("js-nostream")
    later = unique_stream_name("after")
    js = SHARED.jetstream
    begin
      result = js.publish(subject, "x")
      result.should be_a(Alumna::Nats::Error)

      created = must_create_stream(js, later, [subject])
      created.name.should eq(later)
    ensure
      js.delete_stream(later)
    end
  end

  it "does not create a stream from core publish" do
    subject = unique_subject("js-core")
    name = unique_stream_name("core")
    js = SHARED.jetstream
    begin
      SHARED.publish(subject, "core").should be_nil
      SHARED.flush.should be_nil
      created = must_create_stream(js, name, [subject])
      created.messages.should eq(0)
    ensure
      js.delete_stream(name)
    end
  end

  it "raises ArgumentError for an empty stream name" do
    js = SHARED.jetstream
    expect_raises(ArgumentError, "NATS stream name must not be empty") do
      js.create_stream("", [unique_subject("js-empty-name")])
    end
    expect_raises(ArgumentError, "NATS stream name must not be empty") do
      js.stream_info("")
    end
    expect_raises(ArgumentError, "NATS stream name must not be empty") do
      js.delete_stream("")
    end
  end

  it "raises ArgumentError when the stream name contains a dot" do
    expect_raises(ArgumentError, "NATS stream name must not contain '.'") do
      SHARED.jetstream.create_stream("bad.name", [unique_subject("js-dot")])
    end
  end

  it "raises ArgumentError for empty stream subjects" do
    js = SHARED.jetstream
    name = unique_stream_name("nosub")
    expect_raises(ArgumentError, "NATS stream must have at least one subject") do
      js.create_stream(name, [] of String)
    end
    expect_raises(ArgumentError, "NATS subject must not be empty") do
      js.create_stream(name, [""])
    end
  end

  it "raises ArgumentError for an empty or invalid JetStream publish subject" do
    js = SHARED.jetstream
    expect_raises(ArgumentError, "NATS subject must not be empty") do
      js.publish("", "x")
    end
    expect_raises(ArgumentError) do
      js.publish("bad subject", "x")
    end
  end

  it "returns Error for stream helpers after close" do
    holder = connect_nats
    name = unique_stream_name("closed")
    subject = unique_subject("js-closed")
    js = holder.jetstream
    holder.close.should be_nil
    js.create_stream(name, [subject], storage: :memory).should be_a(Alumna::Nats::Error)
    js.stream_info(name).should be_a(Alumna::Nats::Error)
    js.delete_stream(name).should be_a(Alumna::Nats::Error)
    js.publish(subject, "x").should be_a(Alumna::Nats::Error)
  end
end
