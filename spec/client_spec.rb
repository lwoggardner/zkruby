require 'spec_helper'

describe ZooKeeper::Client do

    describe "A local connection" do
        before(:all) do
            @zk = ZooKeeper.connect("localhost:2181",:timeout => 0.2)
            unless @zk.exists("/zkruby")
                @zk.create("/zkruby","node for zk ruby testing",ZK::ACL_OPEN_UNSAFE)
            end
        end

        after(:all) do
            @zk.close()
        end

        it "should return a stat for the root path" do
            stat = @zk.stat("/")
            stat.should be_a ZooKeeper::Data::Stat
        end

        it "should asynchronously return a stat for the root path" do
            @zk.stat("/") do |stat|
                stat.should be_a ZooKeeper::Data::Stat
            end
        end

        it "should perform all the ZooKeeper CRUD" do
            path = @zk.create("/zkruby/rspec","someData",ZK::ACL_OPEN_UNSAFE,:ephemeral)
            path.should == "/zkruby/rspec"
            stat,data = @zk.get("/zkruby/rspec")
            stat.should be_a ZooKeeper::Data::Stat
            data.should == "someData"
            new_stat = @zk.set("/zkruby/rspec","different data",stat.version)
            new_stat.should be_a ZooKeeper::Data::Stat
            stat,data = @zk.get("/zkruby/rspec")
            data.should == "different data"
            @zk.delete("/zkruby/rspec",stat.version)
            @zk.exists?("/zkruby/rspec").should be_false
        end

        it "should raise ZooKeeperErrors in various circumstances"
        it "should call the error call back for asynchronous errors"

        describe "anti herd-effect features" do
            it "should randomly shuffle the address list"

            it "should randomly delay reconnections within one seventh of the timeout"
        end    
        
    end
end
