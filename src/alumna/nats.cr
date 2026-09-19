require "uri"
require "nats"
require "./nats/errors"
require "./nats/jetstream"

# One NATS client for the process. Not a Service adapter.
class Alumna::Nats
  # Inbound core message. *body* is a view; copy it if you keep it after the handler returns.
  struct Message
    getter subject : String
    getter body : Bytes

    def initialize(@subject : String, @body : Bytes)
    end
  end

  # Handle from `subscribe`. Pass it to `unsubscribe`.
  # *queue_group* is nil when the subscribe has no queue group (fan-out).
  struct Subscription
    getter subject : String
    getter queue_group : String?
    getter handle : ::NATS::Subscription

    def initialize(@handle : ::NATS::Subscription)
      @subject = @handle.subject
      @queue_group = @handle.queue_group
    end
  end

  getter client : ::NATS::Client

  # Open a client. *servers* is a URI, a URL string, or a list of servers.
  # Pass *nkeys_file* or *user_credentials* when the server requires them.
  # Returns `Error` when the driver fails. Raises `ArgumentError` for a bad URL.
  def self.new(
    servers : URI | String | Array(URI) | Array(String),
    *,
    nkeys_file : String? = nil,
    user_credentials : String? = nil,
  ) : self | Error
    uris = to_uris(servers)
    raise ArgumentError.new("NATS server list must not be empty") if uris.empty?

    client = ::NATS::Client.new(uris, nkeys_file: nkeys_file, user_credentials: user_credentials)
    holder = allocate
    holder.initialize(client)
    holder
  rescue ex : ArgumentError
    raise ex
  rescue ex
    Errors.wrap(ex)
  end

  def self.from_uri(
    uri : URI | String,
    *,
    nkeys_file : String? = nil,
    user_credentials : String? = nil,
  ) : self | Error
    new(uri, nkeys_file: nkeys_file, user_credentials: user_credentials)
  end

  def self.from_env(
    name : String = "NATS_URL",
    *,
    nkeys_file : String? = nil,
    user_credentials : String? = nil,
  ) : self | Error
    value = ENV[name]?
    if value.nil? || value.empty?
      raise ArgumentError.new("Missing environment variable #{name}")
    end
    new(value, nkeys_file: nkeys_file, user_credentials: user_credentials)
  end

  protected def initialize(@client : ::NATS::Client)
  end

  # Wait for a PONG. Uses flush.
  def ping : Nil | Error
    flush
  end

  # Push the output buffer and wait for a PONG.
  def flush : Nil | Error
    run { @client.flush }
  end

  def close : Nil | Error
    run { @client.close }
  end

  # Core publish. Does not wait for subscribers. Payload is `String` or `Bytes`.
  # Empty or invalid *subject* raises `ArgumentError`. App owns the subject name.
  def publish(subject : String, payload : String | Bytes) : Nil | Error
    check_subject!(subject)
    run { @client.publish(subject, payload) }
  end

  # Core subscribe. Does not block.
  # With no *queue_group*, each current subscriber gets a copy (fan-out).
  # With *queue_group*, subscribers in that group compete (one delivery per message).
  # Empty or invalid *subject*, or empty *queue_group*, raises `ArgumentError`.
  # Wildcards `*` and `>` are valid here. Core NATS does not persist the message.
  def subscribe(subject : String, *, queue_group : String? = nil, &block : Message ->) : Subscription | Error
    check_subject!(subject)
    check_queue_group!(queue_group)
    run do
      handle = @client.subscribe(subject, queue_group: queue_group) do |raw, _sub|
        block.call(Message.new(raw.subject, raw.body))
      end
      Subscription.new(handle)
    end
  end

  def unsubscribe(subscription : Subscription) : Nil | Error
    run { @client.unsubscribe(subscription.handle) }
  end

  private def check_subject!(subject : String) : Nil
    raise ArgumentError.new("NATS subject must not be empty") if subject.empty?
  end

  private def check_queue_group!(queue_group : String?) : Nil
    if queue_group.try(&.empty?)
      raise ArgumentError.new("NATS queue group must not be empty")
    end
  end

  # Programmer mistakes (`ArgumentError`) leave the method. Driver failures become `Error`.
  private def run(& : -> T) : T | Error forall T
    yield
  rescue ex : ArgumentError
    raise ex
  rescue ex
    Errors.wrap(ex)
  end

  private def self.to_uris(servers : URI) : Array(URI)
    check_scheme!(servers)
    [servers]
  end

  private def self.to_uris(servers : String) : Array(URI)
    [parse_uri(servers)]
  end

  private def self.to_uris(servers : Array(URI)) : Array(URI)
    servers.each { |uri| check_scheme!(uri) }
    servers
  end

  private def self.to_uris(servers : Array(String)) : Array(URI)
    servers.map { |value| parse_uri(value) }
  end

  private def self.parse_uri(value : String) : URI
    raise ArgumentError.new("NATS URL must not be empty") if value.empty?
    uri = URI.parse(value)
    check_scheme!(uri)
    uri
  end

  private def self.check_scheme!(uri : URI) : Nil
    scheme = uri.scheme
    unless scheme == "nats" || scheme == "tls"
      raise ArgumentError.new("NATS URL scheme must be nats:// or tls://")
    end
  end
end
