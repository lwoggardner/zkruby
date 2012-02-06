require 'spec_helper'

class ZKBTest < BinData::Record
    int8 :testint
    zk_boolean :bool
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
