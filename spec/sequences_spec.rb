require 'spec_helper'

describe ZooKeeper do
    it "should convert a path to a prefix and the sequence id" do
       
        ZK.path_to_seq("/a/path/with-0000000001/a/sequence-0000000002").should == [ "/a/path/with-0000000001/a/sequence-",2 ]
        ZK.path_to_seq("/tricky/9990000000004").should == [ "/tricky/999", 4 ]
    end

    it "should graciously handle a path without a sequence" do
        ZK.path_to_seq("/a/path").should ==  ["/a/path",nil]
        ZK.path_to_seq("/a/0000000001/").should == ["/a/0000000001/",nil]
    end

    it "should convert to a prefix and sequence id to a path" do
        ZK.seq_to_path("/a/path",345).should == "/a/path0000000345"
        ZK.seq_to_path("/a/path00/",123).should == "/a/path00/0000000123"
    end
end
