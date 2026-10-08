# Operation errors for Alumna NATS. This is a struct, not an Exception.
# Programmer and config mistakes raise ArgumentError.
class Alumna::Nats
  struct Error
    getter message : String

    def initialize(@message : String)
    end

    def to_s(io : IO) : Nil
      io << @message
    end
  end

  module Errors
    # Strip `//user:pass@` so a password in a connection error does not leave the shard.
    # A message with no match is the same String (no copy).
    def self.safe_message(ex : Exception) : String
      msg = ex.message
      return "NATS error" unless msg && !msg.empty?
      strip_userinfo(msg)
    end

    private def self.strip_userinfo(msg : String) : String
      bytes = msg.to_slice
      index = 0
      while index < bytes.size &- 1
        if bytes[index] == 47_u8 && bytes[index &+ 1] == 47_u8 && userinfo_end(bytes, index) >= 0
          return build_stripped(msg, bytes)
        end
        index &+= 1
      end
      msg
    end

    # Index of '@' that ends `//userinfo`, or -1. Stops at '/' or ASCII whitespace.
    private def self.userinfo_end(bytes : Bytes, slash : Int32) : Int32
      index = slash &+ 2
      while index < bytes.size
        byte = bytes[index]
        return -1 if byte == 47_u8 || ascii_space?(byte)
        return index if byte == 64_u8
        index &+= 1
      end
      -1
    end

    private def self.ascii_space?(byte : UInt8) : Bool
      byte == 32_u8 || (byte >= 9_u8 && byte <= 13_u8)
    end

    private def self.build_stripped(msg : String, bytes : Bytes) : String
      String.build(msg.bytesize) do |io|
        index = 0
        while index < bytes.size
          if index < bytes.size &- 1 && bytes[index] == 47_u8 && bytes[index &+ 1] == 47_u8
            at = userinfo_end(bytes, index)
            if at >= 0
              io.write_byte 47_u8
              io.write_byte 47_u8
              index = at &+ 1
              next
            end
          end
          io.write_byte bytes[index]
          index &+= 1
        end
      end
    end

    def self.wrap(ex : Exception) : Error
      Error.new(safe_message(ex))
    end
  end
end
