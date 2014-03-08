require 'spec_helper'

def tohex(s)
    s.unpack("H*")[0]
end

describe ZooKeeper::Proto::MultiRequest do

    it "an empty multi should match the java output" do

        java_hex = "ffffffff01ffffffff"
        java_bin = [ java_hex ].pack("H*")

        read_request = ZooKeeper::Proto::MultiRequest.read(java_bin)

        read_hex = tohex(read_request.to_binary_s)

        m = ZooKeeper::Proto::MultiRequest.new()

        m.requests << { :header => { :_type => -1, :done => true, :err => -1 } }

        m_hex = tohex(m.to_binary_s)
        m_hex.should == java_hex
        m.should == read_request
    end

    it "should match the java output with records" do

        java_hex = "0000000d00ffffffff00000005636865636b000000010000000100ffffffff000000066372656174650000000b6372656174652064617461000000010000001f000000046175746800000000000000000000000200ffffffff0000000664656c657465000000110000000500ffffffff000000077365744461746100000008736574206461746100000013ffffffff01ffffffff"

        java_bin = [ java_hex ].pack("H*")

        read_request = ZooKeeper::Proto::MultiRequest.read(java_bin)

        read_hex = tohex(read_request.to_binary_s)

        read_hex.should == java_hex

        m = ZooKeeper::Proto::MultiRequest.new()

        check = ZK::Proto::CheckVersionRequest.new( :path => "check", :version => 1 )
        m.requests << { :header => { :_type => 13, :done => false, :err => -1 }, :request => check }

        create = ZK::Proto::CreateRequest.new( :path => "create", :data => "create data", :acl => ZK::CREATOR_ALL_ACL, :flags => 0)
        m.requests << { :header => { :_type => 1, :done => false, :err => -1 }, :request => create }

        delete = ZK::Proto::DeleteRequest.new( :path => "delete", :version => 17)
        m.requests << { :header => { :_type => 2, :done => false, :err => -1 }, :request => delete }


        set = ZK::Proto::SetDataRequest.new( :path => "setData", :data => "set data", :version => 19)
        m.requests << { :header => { :_type => 5, :done => false, :err => -1 }, :request => set }

        m.requests << { :header => { :_type => -1, :done => true, :err => -1 } }

        m_hex = tohex(m.to_binary_s)
        m_hex.should == java_hex
        m.should == read_request
    end

    it "should decode a response" do

        response_hex = "000000010000000000000000062f6d756c7469ffffffff01ffffffff"

        response_bin = [ response_hex ].pack("H*")

        m = ZooKeeper::Proto::MultiResponse.read(response_bin)
        m.responses.size.should == 2
        m.responses[0].done?.should be_false
        m.responses[0].header.err.should == 0
        m.responses[0].header._type.should == 1
        m.responses[0].response.path.should == "/multi"
        m.responses[1].done?.should be_true

    end

    it "should decode an error response" do
        response_hex = "ffffffff000000000000000000ffffffff00ffffff9bffffff9bffffffff01ffffffff"

        response_bin = [ response_hex ].pack("H*")

        m = ZooKeeper::Proto::MultiResponse.read(response_bin)
        m.responses.size.should == 3
        m.responses[0].header._type.should == -1
        m.responses[0].header.err.should == 0
        m.responses[0].done?.should be_false
        m.responses[0].response.err.should == 0
        m.responses[1].header._type.should == -1
        m.responses[1].header.err.should == -101
        m.responses[1].response.err.should == -101
        m.responses[2].done?.should be_true
    end
end
