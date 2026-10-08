require "nats/jetstream"

class Alumna::Nats
  # JetStream helper. You must create a stream and a durable push consumer.
  # Publish does not create a stream. Subscribe does not create a consumer.
  # The handler does not ack.
  # For a job queue, create the stream with `retention: :workqueue`.
  # One helper for this client. Later calls return the same object.
  def jetstream : JetStream
    @jetstream ||= JetStream.new(@client)
  end

  class JetStream
    enum Storage
      File
      Memory
    end

    # How the server drops stored messages.
    # Limits: keep until size or age. Independent consumers can each keep a copy.
    # Interest: keep while a consumer is interested.
    # Workqueue: the job queue. The first ack removes the message.
    enum Retention
      Limits
      Interest
      Workqueue
    end

    struct Stream
      getter name : String
      getter subjects : Array(String)
      getter storage : Storage
      getter retention : Retention
      getter messages : Int64

      def initialize(@name : String, @subjects : Array(String), @storage : Storage, @retention : Retention, @messages : Int64)
      end

      def self.from_driver(raw : ::NATS::JetStream::Stream) : self
        new(
          raw.config.name,
          raw.config.subjects,
          storage_from_driver(raw.config.storage),
          retention_from_driver(raw.config.retention),
          raw.state.messages,
        )
      end

      private def self.storage_from_driver(value : ::NATS::JetStream::StreamConfig::Storage) : Storage
        case value
        in .memory?
          Storage::Memory
        in .file?
          Storage::File
        end
      end

      private def self.retention_from_driver(value : ::NATS::JetStream::StreamConfig::RetentionPolicy?) : Retention
        if value && value.workqueue?
          Retention::Workqueue
        elsif value && value.interest?
          Retention::Interest
        else
          Retention::Limits
        end
      end
    end

    struct PubAck
      getter stream : String
      getter sequence : Int64
      getter duplicate : Bool

      def initialize(@stream : String, @sequence : Int64, @duplicate : Bool)
      end

      def self.from_driver(raw : ::NATS::JetStream::PubAck) : self
        new(raw.stream, raw.sequence, raw.duplicate?)
      end
    end

    # Durable push consumer. *deliver_subject* is nil for a pull consumer (not in this product).
    struct Consumer
      getter stream : String
      getter name : String
      getter deliver_subject : String?
      getter deliver_group : String?
      getter filter_subject : String?
      getter num_pending : Int64
      getter num_ack_pending : Int64

      def initialize(
        @stream : String,
        @name : String,
        @deliver_subject : String?,
        @deliver_group : String?,
        @filter_subject : String?,
        @num_pending : Int64,
        @num_ack_pending : Int64,
      )
      end

      def self.from_driver(raw : ::NATS::JetStream::Consumer) : self
        new(
          raw.stream_name,
          raw.name,
          raw.config.deliver_subject,
          raw.config.deliver_group,
          raw.config.filter_subject,
          raw.num_pending,
          raw.num_ack_pending,
        )
      end
    end

    # Inbound JetStream message.
    # *payload* is the server text. It stays valid while you keep this value.
    # *body* is a byte view of *payload*. Copy *body* if you keep the bytes and drop this value.
    # Pass the same value to `ack` or `nack`. The handler does not ack.
    struct Message
      getter subject : String
      getter payload : String
      getter stream : String
      getter consumer : String
      getter delivered_count : Int64
      getter reply_to : String

      def initialize(
        @subject : String,
        @payload : String,
        @stream : String,
        @consumer : String,
        @delivered_count : Int64,
        @reply_to : String,
      )
      end

      def body : Bytes
        @payload.to_slice
      end

      def self.from_driver(raw : ::NATS::JetStream::Message) : self
        new(raw.subject, raw.data_string, raw.stream, raw.consumer, raw.delivered_count, raw.reply_to)
      end
    end

    def initialize(@client : ::NATS::Client)
    end

    # Create a stream. *name* must not be empty and must not contain '.'.
    # Default *retention* is limits. Pass `:workqueue` for the job queue:
    # the first ack removes the message, and a second consumer on the same
    # interest returns Error.
    def create_stream(
      name : String,
      subjects : Array(String),
      *,
      storage : Storage = :file,
      retention : Retention = :limits,
    ) : Stream | Alumna::Nats::Error
      check_stream_name!(name)
      check_stream_subjects!(subjects)
      run do
        raw = @client.jetstream.stream.create(
          name: name,
          subjects: subjects,
          storage: storage_to_driver(storage),
          retention: retention_to_driver(retention),
        )
        Stream.from_driver(raw)
      end
    end

    # Return the stream, or nil if it does not exist.
    def stream_info(name : String) : Stream? | Alumna::Nats::Error
      check_stream_name!(name)
      run do
        raw = @client.jetstream.stream.info(name)
        raw ? Stream.from_driver(raw) : nil
      end
    end

    # Remove the stream. If the stream does not exist, this is a no-op.
    def delete_stream(name : String) : Nil | Alumna::Nats::Error
      check_stream_name!(name)
      run do
        @client.jetstream.stream.delete(name)
        nil
      end
    end

    # Publish to a stream that already listens on *subject*.
    # If no stream listens, return Error. This does not create a stream.
    def publish(subject : String, payload : String | Bytes) : PubAck | Alumna::Nats::Error
      raise ArgumentError.new("NATS subject must not be empty") if subject.empty?
      run do
        raw = @client.jetstream.publish!(subject, payload)
        PubAck.from_driver(raw)
      end
    end

    # Create a durable push consumer. Does not create a stream.
    # Default *deliver_group* is *name* so workers on this consumer compete.
    # Default *deliver_subject* is generated from the stream and consumer names.
    # Deliver policy is all: a late consumer receives stored messages.
    # Optional *ack_wait* is how long an unacked message waits before redelivery.
    # On a workqueue stream, one consumer per interest. Extra overlapping
    # consumers return Error. Workers share this consumer.
    def create_consumer(
      stream : String,
      name : String,
      *,
      deliver_subject : String? = nil,
      deliver_group : String? = nil,
      filter_subject : String? = nil,
      ack_wait : Time::Span? = nil,
    ) : Consumer | Alumna::Nats::Error
      check_stream_name!(stream)
      check_consumer_name!(name)
      check_optional_name!(deliver_subject, "NATS deliver subject must not be empty")
      check_optional_name!(deliver_group, "NATS deliver group must not be empty")
      check_optional_name!(filter_subject, "NATS filter subject must not be empty")
      check_ack_wait!(ack_wait)
      run do
        subject = deliver_subject || default_deliver_subject(stream, name)
        group = deliver_group || name
        raw = @client.jetstream.consumer.create(
          stream_name: stream,
          durable_name: name,
          deliver_subject: subject,
          deliver_group: group,
          filter_subject: filter_subject,
          ack_policy: :explicit,
          deliver_policy: :all,
          ack_wait: ack_wait,
        )
        Consumer.from_driver(raw)
      end
    end

    # Return the consumer, or nil if it does not exist.
    def consumer_info(stream : String, name : String) : Consumer? | Alumna::Nats::Error
      check_stream_name!(stream)
      check_consumer_name!(name)
      run do
        raw = @client.jetstream.consumer.info(stream, name)
        raw ? Consumer.from_driver(raw) : nil
      end
    end

    # Remove the consumer. If the consumer does not exist, this is a no-op.
    def delete_consumer(stream : String, name : String) : Nil | Alumna::Nats::Error
      check_stream_name!(stream)
      check_consumer_name!(name)
      run do
        @client.jetstream.consumer.delete(stream, name)
        nil
      end
    end

    # Push subscribe. Does not block. Does not create a consumer. Does not ack.
    # *consumer* must have a deliver subject (push). Pull consumers raise ArgumentError.
    def subscribe(consumer : Consumer, &block : Message ->) : Alumna::Nats::Subscription | Alumna::Nats::Error
      subject = consumer.deliver_subject
      if subject.nil? || subject.empty?
        raise ArgumentError.new("NATS consumer must be a push consumer")
      end
      run do
        handle = @client.jetstream.subscribe(subject, queue_group: consumer.deliver_group) do |raw, _sub|
          block.call(Message.from_driver(raw))
        end
        Alumna::Nats::Subscription.new(handle)
      end
    end

    def unsubscribe(subscription : Alumna::Nats::Subscription) : Nil | Alumna::Nats::Error
      run { @client.unsubscribe(subscription.handle) }
    end

    # Acknowledge the message. The handler does not ack when it returns.
    def ack(msg : Message) : Nil | Alumna::Nats::Error
      check_reply_to!(msg)
      run { @client.publish(msg.reply_to, "+ACK") }
    end

    # Reject the message so the server can deliver it again.
    # *delay* waits before the next delivery.
    def nack(msg : Message, *, delay : Time::Span? = nil) : Nil | Alumna::Nats::Error
      check_reply_to!(msg)
      if delay
        raise ArgumentError.new("NATS nack delay must be greater than zero") if delay <= Time::Span.zero
        # 14 byte prefix, at most 19 digits, one closing brace.
        buf = uninitialized UInt8[40]
        len = write_nack_delay(buf.to_unsafe, delay)
        # `as(Nil)` fixes the block return type for this Slice argument.
        run { @client.publish(msg.reply_to, Slice.new(buf.to_unsafe, len)).as(Nil) }
      else
        run { @client.publish(msg.reply_to, "-NAK") }
      end
    end

    private def check_stream_name!(name : String) : Nil
      raise ArgumentError.new("NATS stream name must not be empty") if name.empty?
      if name.includes?('.')
        raise ArgumentError.new("NATS stream name must not contain '.'")
      end
    end

    private def check_consumer_name!(name : String) : Nil
      raise ArgumentError.new("NATS consumer name must not be empty") if name.empty?
      if name.includes?('.')
        raise ArgumentError.new("NATS consumer name must not contain '.'")
      end
    end

    private def check_optional_name!(value : String?, message : String) : Nil
      if value && value.empty?
        raise ArgumentError.new(message)
      end
    end

    private def check_reply_to!(msg : Message) : Nil
      raise ArgumentError.new("NATS JetStream ack subject must not be empty") if msg.reply_to.empty?
    end

    private def check_ack_wait!(wait : Time::Span?) : Nil
      return unless wait
      raise ArgumentError.new("NATS ack wait must be greater than zero") if wait <= Time::Span.zero
    end

    private def default_deliver_subject(stream : String, name : String) : String
      "_ALUMNA.JS.#{stream}.#{name}"
    end

    private def write_nack_delay(buf : Pointer(UInt8), delay : Time::Span) : Int32
      prefix = "-NAK {\"delay\":".to_slice
      buf.copy_from(prefix.to_unsafe, prefix.size)
      len = prefix.size
      n = delay.total_nanoseconds.to_u64
      digits = uninitialized UInt8[20]
      count = 0
      while n > 0
        digits[count] = 48_u8 &+ (n % 10).to_u8
        count &+= 1
        n //= 10
      end
      index = count
      while index > 0
        index &-= 1
        buf[len] = digits[index]
        len &+= 1
      end
      buf[len] = 125_u8
      len &+= 1
      len
    end

    private def check_stream_subjects!(subjects : Array(String)) : Nil
      raise ArgumentError.new("NATS stream must have at least one subject") if subjects.empty?
      subjects.each do |subject|
        raise ArgumentError.new("NATS subject must not be empty") if subject.empty?
      end
    end

    private def storage_to_driver(storage : Storage) : ::NATS::JetStream::StreamConfig::Storage
      case storage
      in .memory?
        ::NATS::JetStream::StreamConfig::Storage::Memory
      in .file?
        ::NATS::JetStream::StreamConfig::Storage::File
      end
    end

    private def retention_to_driver(retention : Retention) : ::NATS::JetStream::StreamConfig::RetentionPolicy
      case retention
      in .limits?
        ::NATS::JetStream::StreamConfig::RetentionPolicy::Limits
      in .interest?
        ::NATS::JetStream::StreamConfig::RetentionPolicy::Interest
      in .workqueue?
        ::NATS::JetStream::StreamConfig::RetentionPolicy::Workqueue
      end
    end

    private def run(& : -> T) : T | Alumna::Nats::Error forall T
      yield
    rescue ex : ArgumentError
      raise ex
    rescue ex
      Errors.wrap(ex)
    end
  end
end
