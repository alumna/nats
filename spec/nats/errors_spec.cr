require "../spec_helper"

describe Alumna::Nats::Errors do
  it "returns a generic message when the exception has no text" do
    Alumna::Nats::Errors.safe_message(Exception.new).should eq("NATS error")
    Alumna::Nats::Errors.safe_message(Exception.new("")).should eq("NATS error")
  end

  it "returns the same string when the message has no userinfo" do
    msg = "connection refused"
    ex = Exception.new(msg)
    Alumna::Nats::Errors.safe_message(ex).should be(ex.message)

    at = Exception.new("user@host timed out")
    Alumna::Nats::Errors.safe_message(at).should be(at.message)

    url = Exception.new("see http://example.com/a@b")
    Alumna::Nats::Errors.safe_message(url).should be(url.message)

    spaced = Exception.new("see http://foo bar@baz")
    Alumna::Nats::Errors.safe_message(spaced).should be(spaced.message)
  end

  it "strips URI userinfo from a message" do
    raw = Exception.new("failed nats://user:secret@127.0.0.1:4222 extra")
    safe = Alumna::Nats::Errors.safe_message(raw)
    safe.includes?("secret").should be_false
    safe.includes?("user:").should be_false
    safe.should eq("failed nats://127.0.0.1:4222 extra")

    tls = Exception.new("tls tls://:hunter2@nats.example.com:4222")
    Alumna::Nats::Errors.safe_message(tls).should eq("tls tls://nats.example.com:4222")

    both = Exception.new("a nats://u:p@h b tls://x:y@z")
    Alumna::Nats::Errors.safe_message(both).should eq("a nats://h b tls://z")

    empty_user = Exception.new("failed nats://@host and http://example.com")
    Alumna::Nats::Errors.safe_message(empty_user).should eq("failed nats://host and http://example.com")
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
