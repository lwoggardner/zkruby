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

class ZKBoolean < BinData::Primitive
   int8 :boolvalue

   def get; self.boolvalue != 0; end
   def set(v) self.boolvalue = (v ? 1 : 0 ); end
end
