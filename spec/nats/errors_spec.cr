require "../spec_helper"

describe Alumna::Nats::Errors do
  it "returns a generic message when the exception has no text" do
    Alumna::Nats::Errors.safe_message(Exception.new).should eq("NATS error")
    Alumna::Nats::Errors.safe_message(Exception.new("")).should eq("NATS error")
  end

  it "strips URI userinfo from a message" do
    raw = Exception.new("failed nats://user:secret@127.0.0.1:4222 extra")
    safe = Alumna::Nats::Errors.safe_message(raw)
    safe.includes?("secret").should be_false
    safe.includes?("user:").should be_false
    safe.should eq("failed nats://127.0.0.1:4222 extra")

    tls = Exception.new("tls tls://:hunter2@nats.example.com:4222")
    Alumna::Nats::Errors.safe_message(tls).should eq("tls tls://nats.example.com:4222")
  end

  it "wraps a driver exception as Alumna::Nats::Error" do
    wrapped = Alumna::Nats::Errors.wrap(Exception.new("boom nats://u:secret@host"))
    wrapped.should be_a(Alumna::Nats::Error)
    wrapped.message.includes?("secret").should be_false
    wrapped.message.should eq("boom nats://host")
    wrapped.to_s.should eq("boom nats://host")
  end
end

describe "spec NATS URL display" do
  it "strips userinfo from a spec abort URL" do
    display_nats_url("nats://user:secret@127.0.0.1:4222").includes?("secret").should be_false
    display_nats_url("not a uri ://").should_not be_nil
  end
end
