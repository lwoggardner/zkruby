require 'spec_helper'

# Raw object protocol, very similar to EM's native ObjectProtocol
# Expects:
#   #receive_data and #send_records to be invoked
#   #receive_records and #send_data to be implemented
class ProtocolBindingSpec
    include ZooKeeper::Protocol
    attr_reader :data,:binding

    def initialize(mock)
        @data = ""
        @binding = mock
    end

    def receive_records(io)
       data = io.read(@binding.packet_size())
       @binding.receive_records(data)
    end

    def send_data(data)
        @data << data
    end
end

def length_encode(data)
    [data.length,data].pack("NA*")
end

describe ZooKeeper::Protocol do

    before :each do
            @conn = ProtocolBindingSpec.new(mock("protocol binding"))
    end

    context "recieving data" do
        it "should handle self contained packets" do
            data = "a complete packet"
            @conn.binding.stub(:packet_size).and_return(data.length)
            @conn.binding.should_receive(:receive_records).with(data)
            @conn.receive_data(length_encode(data))
        end

        it "should handle reply packets over multiple chunks" do
            data = "a complete packet that will be split into chunks"
            @conn.binding.stub(:packet_size).and_return(data.length)
            @conn.binding.should_receive(:receive_records).with(data)
            chunks = length_encode(data).scan(/.{1,12}/)
            chunks.each { |c| @conn.receive_data(c) }
        end

        it "should handle replies containing multiple packets" do
            p1 = "first packet"
            p2 = "second packet"
            @conn.binding.stub(:packet_size).and_return(p1.length,p2.length)
            @conn.binding.should_receive(:receive_records).with(p1).ordered
            @conn.binding.should_receive(:receive_records).with(p2).ordered
            data = length_encode(p1) + length_encode(p2)
            @conn.receive_data(data)
        end
    end

    context "sending data" do
        it "should length encode the data" do
            mock_record = mock("a mock record")
            mock_record.stub(:to_binary_s).and_return("a binary string")
            @conn.send_records(mock_record)
            @conn.data.should ==  [15,"a binary string"].pack("NA15")
            
        end
    end
end
