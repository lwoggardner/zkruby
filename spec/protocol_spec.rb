require 'spec_helper'

# Raw object protocol, very similar to EM's native ObjectProtocol
# Expects:
#   #receive_data and #send_records to be invoked
#   #receive_records and #send_data to be implemented

describe ZooKeeper::Protocol do

    context "recieving data" do
        it "should handle self contained packets"

        it "should handle reply packets over multiple chunks"

        it "should handle replies containing multiple packets"
    end

    context "sending data" do
        it "should length encode the data"
    end
end
