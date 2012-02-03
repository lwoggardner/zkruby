#TODO This belongs to jute rather than zookeeper and ideally would be a separate project/gem
require 'bindata'

class ZKBuffer < BinData::Primitive
  int32be  :len,  :value => lambda { data.nil? ? -1 : data.length }
  string :data, :read_length => :len

  def get;   self.data; end
  def set(v) self.data = v; end
end

class ZKString < BinData::Primitive
  int32be  :len,  :value => lambda { data.nil? ? -1 : data.length }
  string :data, :read_length => :len

  def get;   self.data; end
  def set(v) self.data = v; end
  def snapshot
      super.force_encoding('UTF-8')
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
