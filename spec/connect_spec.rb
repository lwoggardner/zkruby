require 'spec_helper'

describe ZooKeeper::Client do

        before(:all) do
            restart_cluster(2)
            @zk = connect()
            unless @zk.exists("/zkruby")
                @zk.create("/zkruby","node for zk ruby testing",ZK::ACL_OPEN_UNSAFE)
            end
        end

        after(:all) do
            safe_close(@zk)
        end

        it "should stay connected" do
            sleep(@zk.timeout * 2.0)
            @zk.exists?("/zkruby").should be_true
        end


        it "should seamlessly reconnect within the timeout period" do
            watcher = mock("Watcher").as_null_object
            watcher.should_receive(:process_watch).with(ZK::KeeperState::DISCONNECTED,nil,ZK::WatchEvent::NONE)
            watcher.should_receive(:process_watch).with(ZK::KeeperState::CONNECTED,nil,ZK::WatchEvent::NONE)
            watcher.should_not_receive(:process_watch).with(ZK::KeeperState::EXPIRED,nil,ZK::WatchEvent::NONE)
            @zk.watcher = watcher
            restart_cluster(2)
            @zk.exists?("/zkruby").should be_true
        end
        
        it "should eventually expire the session" do
            watcher = mock("Watcher").as_null_object
            watcher.should_receive(:process_watch).with(ZK::KeeperState::DISCONNECTED,nil,ZK::WatchEvent::NONE)
            watcher.should_receive(:process_watch).with(ZK::KeeperState::EXPIRED,nil,ZK::WatchEvent::NONE)
            @zk.watcher = watcher
            restart_cluster(@zk.timeout * 2.0)
            lambda { @zk.exists?("/zkruby") }.should raise_error(ZooKeeperError)
        end

end
