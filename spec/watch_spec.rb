
require 'spec_helper'

describe "watches" do
    before (:all) do
        @zk = ZooKeeper.connect("localhost:2181")
        @zk2 = ZooKeeper.connect("localhost:2181")
    end

    after(:all) do
        @zk.close()
        @zk2.close()
    end

    it "should handle data watches" do
        path = @zk.create("/zkruby/rspec_watch","somedata",ZK::ACL_OPEN_UNSAFE,:ephemeral,:sequential)
        watch_results = []
        watch = lambda { |state,path,event| watch_results << [ state,path,event ] }
        
        stat,data = @zk.get(path,watch)
        # set the data on the 2nd session
        @zk2.set(path,"newdata",stat.version)
        sleep(5)
        watch_results.size().should == 1
        watch_results[0][1].should == path
    end

    it "should handle exists watches"
    it "should handle child watches"

    it "should reset watches over socket disconnects"
    it "should not send :connection_lost to watches on disconnect if so configured"

    describe :close do
        it "should send :session_expired to the default watcher on close"
        it "should send :Session_expired to all watchers on close"
    end
end
