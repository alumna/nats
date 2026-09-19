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
    USERINFO = /\/\/[^\/\s]*@/

    def self.safe_message(ex : Exception) : String
      msg = ex.message
      return "NATS error" unless msg && !msg.empty?

      msg.gsub(USERINFO, "//")
    end

    def self.wrap(ex : Exception) : Error
      Error.new(safe_message(ex))
    end
  end
end
