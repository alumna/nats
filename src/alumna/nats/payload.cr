require "alumna"

class Alumna::Nats
  # JSON for AnyData. Writes into *io*. No intermediate String.
  private def write_payload(io : IO, value : Alumna::AnyData) : Nil
    case value
    when Nil
      io << "null"
    when Bool
      io << (value ? "true" : "false")
    when Int64
      io << value
    when Float64
      if value.nan? || value.infinite?
        raise ArgumentError.new("NATS JSON number must be finite")
      end
      io << value
    when String
      write_payload_string(io, value)
    when Time
      io << '"'
      Time::Format::RFC_3339.format(value, io, fraction_digits: 0)
      io << '"'
    when Bytes
      io << '['
      index = 0
      while index < value.size
        io << ',' if index > 0
        io << value[index]
        index &+= 1
      end
      io << ']'
    when Array
      io << '['
      index = 0
      while index < value.size
        io << ',' if index > 0
        write_payload(io, value[index])
        index &+= 1
      end
      io << ']'
    when Hash
      io << '{'
      first = true
      value.each do |key, item|
        if first
          first = false
        else
          io << ','
        end
        write_payload_string(io, key)
        io << ':'
        write_payload(io, item)
      end
      io << '}'
    end
  end

  private def write_payload_string(io : IO, value : String) : Nil
    io << '"'
    bytes = value.to_slice
    start = 0
    index = 0
    while index < bytes.size
      byte = bytes[index]
      if byte >= 0x20 && byte != 0x7f && byte != 34 && byte != 92
        index &+= 1
        next
      end
      io.write(bytes[start, index - start]) if index > start
      case byte
      when 92_u8 then io << "\\\\"
      when 34_u8 then io << "\\\""
      when  8_u8 then io << "\\b"
      when 12_u8 then io << "\\f"
      when 10_u8 then io << "\\n"
      when 13_u8 then io << "\\r"
      when  9_u8 then io << "\\t"
      else
        io << "\\u00"
        io << '0' if byte < 0x10
        byte.to_s(io, 16)
      end
      index &+= 1
      start = index
    end
    io.write(bytes[start, bytes.size - start]) if start < bytes.size
    io << '"'
  end
end
