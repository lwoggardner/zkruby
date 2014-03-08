require 'spec_helper'

class ZKBTest < BinData::Record
    int8 :testint
    zk_boolean :bool
end

class ZKBufferTest < BinData::Record
    zk_buffer :buff
end
class ZKStringTest < BinData::Record
    zk_string :str
end

describe ZK::ZKBuffer do

    it "should read null values" do
        hex = "ffffffff"
        bin = [ hex ].pack("H*")

        buffer = ZKBufferTest.read(bin)

        buffer.buff.should == nil
        buffer.buff.value.should be_nil
    end

    it "should write null values" do
        buffer = ZK::ZKBuffer.new(nil)
        buffer.value.should be_nil
        buffer.to_binary_s.should == [ "ffffffff" ].pack("H*")
    end

    it "should read non null values as binary strings"
    it "should write non null values"
    it "should write encoded calues as binary strings"
end

describe ZK::ZKString do
    it "should produce UTF8 encoded strings" do

        hex = "000000086162636465666768"
        bin = [ hex ].pack("H*")
        zk_string = ZKStringTest.read(bin)
        zk_string.str.should == "abcdefgh"
        zk_string.str.encoding.name.should == "UTF-8"

    end
end

describe ZK::ZKBoolean do

    it "should behave like 'false' after reading 00" do
        hex = "0000"
        bin = [ hex ].pack("H*")

        b = ZKBTest.read(bin)

        b.bool.should == false
        # NOTE!! this is unfortunate consequence of Ruby's view of FALSE
        b.bool.should_not be_false

        b.to_binary_s.unpack("H*")[0].should == hex
    end

    it "should behave like 'true' after reading 01" do
        hex = "0001"
        bin = [ hex ].pack("H*")

        b = ZKBTest.read(bin)

        b.bool.should == true
        # NOTE!! this is unfortunate consequence of Ruby's view of FALSE
        b.bool.should be_true

        b.to_binary_s.unpack("H*")[0].should == hex
    end

    it "should behave like a boolean after creating" do

        b = ZKBTest.new()
        b.bool= true
        b.bool.should == true

        b.bool= false
        b.bool.should == false

        b = ZKBTest.new( :bool => true  )
        b.bool.should == true

        b = ZKBTest.new( :bool => false )
        b.bool.should == false
    end
end
