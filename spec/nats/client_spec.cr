require "../spec_helper"

describe Alumna::Nats do
  it "pings a live server from a String URL" do
    holder = connect_nats(NATS_URL)
    holder.client.should be_a(::NATS::Client)
    holder.ping.should be_nil
    holder.close.should be_nil
  end

  it "pings a live server from a URI object" do
    holder = connect_nats(URI.parse(NATS_URL))
    holder.ping.should be_nil
    holder.flush.should be_nil
    holder.close.should be_nil
  end

  it "connects from an array of URI objects" do
    holder = connect_nats([URI.parse(NATS_URL)])
    holder.ping.should be_nil
    holder.close
  end

  it "connects from an array of URL strings" do
    holder = connect_nats([NATS_URL])
    holder.ping.should be_nil
    holder.close
  end

  it "passes nkeys_file and user_credentials to the driver" do
    holder = connect_nats(NATS_URL, nkeys_file: "/tmp/alumna-nats-nkeys", user_credentials: "/tmp/alumna-nats-creds")
    holder.ping.should be_nil
    holder.close
  end

  it "builds from_uri with a URI and with a String" do
    from_obj = Alumna::Nats.from_uri(URI.parse(NATS_URL))
    if from_obj.is_a?(Alumna::Nats::Error)
      fail from_obj.message
    end
    from_obj.ping.should be_nil
    from_obj.close

    from_str = Alumna::Nats.from_uri(NATS_URL)
    if from_str.is_a?(Alumna::Nats::Error)
      fail from_str.message
    end
    from_str.ping.should be_nil
    from_str.close
  end

  it "builds from_env with NATS_URL and with a custom name" do
    old = ENV["NATS_URL"]?
    ENV["NATS_URL"] = NATS_URL
    begin
      holder = Alumna::Nats.from_env
      if holder.is_a?(Alumna::Nats::Error)
        fail holder.message
      end
      holder.ping.should be_nil
      holder.close
    ensure
      if old
        ENV["NATS_URL"] = old
      else
        ENV.delete("NATS_URL")
      end
    end

    ENV["ALUMNA_NATS_SPEC"] = NATS_URL
    begin
      holder = Alumna::Nats.from_env("ALUMNA_NATS_SPEC")
      if holder.is_a?(Alumna::Nats::Error)
        fail holder.message
      end
      holder.ping.should be_nil
      holder.close
    ensure
      ENV.delete("ALUMNA_NATS_SPEC")
    end
  end

  it "raises ArgumentError when from_env is missing or empty" do
    ENV.delete("ALUMNA_NATS_MISSING")
    expect_raises(ArgumentError, "Missing environment variable ALUMNA_NATS_MISSING") do
      Alumna::Nats.from_env("ALUMNA_NATS_MISSING")
    end

    ENV["ALUMNA_NATS_EMPTY"] = ""
    begin
      expect_raises(ArgumentError, "Missing environment variable ALUMNA_NATS_EMPTY") do
        Alumna::Nats.from_env("ALUMNA_NATS_EMPTY")
      end
    ensure
      ENV.delete("ALUMNA_NATS_EMPTY")
    end
  end

  it "raises ArgumentError for an empty URL or empty server list" do
    expect_raises(ArgumentError, "NATS URL must not be empty") do
      Alumna::Nats.new("")
    end
    expect_raises(ArgumentError, "NATS server list must not be empty") do
      Alumna::Nats.new([] of String)
    end
    expect_raises(ArgumentError, "NATS server list must not be empty") do
      Alumna::Nats.new([] of URI)
    end
  end

  it "raises ArgumentError for a URL scheme that is not nats or tls" do
    expect_raises(ArgumentError, "NATS URL scheme must be nats:// or tls://") do
      Alumna::Nats.new("http://127.0.0.1:4222")
    end
    expect_raises(ArgumentError, "NATS URL scheme must be nats:// or tls://") do
      Alumna::Nats.new(URI.parse("http://127.0.0.1:4222"))
    end
    expect_raises(ArgumentError, "NATS URL scheme must be nats:// or tls://") do
      Alumna::Nats.new([URI.parse("http://127.0.0.1:4222")])
    end
  end

  it "returns Error without URI userinfo when the server is down" do
    result = Alumna::Nats.new(dead_url)
    result.should be_a(Alumna::Nats::Error)
    if result.is_a?(Alumna::Nats::Error)
      result.message.includes?("secret").should be_false
      result.message.includes?("user:").should be_false
    end
  end

  it "returns Error when the scheme is tls but the server is plain TCP" do
    result = Alumna::Nats.new("tls://127.0.0.1:4222")
    result.should be_a(Alumna::Nats::Error)
  end

  it "returns Error on ping and flush after close" do
    holder = connect_nats
    holder.close.should be_nil
    ping = holder.ping
    ping.should be_a(Alumna::Nats::Error)
    flush = holder.flush
    flush.should be_a(Alumna::Nats::Error)
    holder.close.should be_nil
  end

  it "pings the shared holder" do
    SHARED.ping.should be_nil
  end
end
