#TODO This belongs to jute rather than zookeeper and ideally would be a separate project/gem
require 'bindata'
module ZooKeeper

  class ZKBuffer < BinData::BasePrimitive

    def value_to_binary_string(v)
      vlen = v.nil? ? -1 : v.length
      [ vlen, v ].pack("NA*")
    end

    def read_and_return_value(io)
      vlen = io.readbytes(4).unpack("i!>")[0]
      vlen < 0 ? nil : io.readbytes(vlen)
    end

    def sensible_default
      nil
    end
  end

  class ZKString < ZKBuffer
    def read_and_return_value(io)
      value = super
      value.force_encoding('UTF-8') unless value.nil?
    end

    def sensible_default
      ""
    end
  end

  #This doesn't work as expected, because when used in a record
  # you get a ZKBoolean instance back which cannot be compared to "false"
  # you must call snapshot or compare with == false
  class ZKBoolean < BinData::BasePrimitive

    def value_to_binary_string(v)
      intval = v ? 1 : 0
      [ intval ].pack("C")
    end

    def read_and_return_value(io)
      intval = io.readbytes(1).unpack("C").at(0)
      intval != 0
    end

    def sensible_default
      false
    end

  end

end
