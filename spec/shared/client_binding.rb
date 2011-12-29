require 'spec_helper'
require 'recipes/util'

shared_examples_for "a zookeeper client binding" do

    let (:binding) { described_class }

    context "A local connection" do
        before(:all) do
            restart_cluster(2)
        end

        before(:each) do
            @zk = connect(:binding => binding)
            unless @zk.exists?("/zkruby")
                @zk.create("/zkruby","node for zk ruby testing",ZK::ACL_OPEN_UNSAFE)
            end
        end

        after(:each) do
            safe_close(@zk)
        end

        it "should return a stat for the root path" do
            stat = @zk.stat("/")
            stat.should be_a ZooKeeper::Data::Stat
        end

        it "should asynchronously return a stat for the root path" do
            op = @zk.stat("/") do |stat|
                begin
                    stat.should be_a ZooKeeper::Data::Stat
                    op.results = true
                rescue Exception => ex
                    op.results = ex
                end
            end

            result = op.waitfor()
            result.should == true
        end

        it "should perform all the ZooKeeper CRUD" do
            path = @zk.create("/zkruby/rspec","someData",ZK::ACL_OPEN_UNSAFE,:ephemeral)
            path.should == "/zkruby/rspec"
            @zk.exists?("/zkruby/rspec").should be_true
            stat,data = @zk.get("/zkruby/rspec")
            stat.should be_a ZooKeeper::Data::Stat
            data.should == "someData"
            new_stat = @zk.set("/zkruby/rspec","different data",stat.version)
            new_stat.should be_a ZooKeeper::Data::Stat
            stat,data = @zk.get("/zkruby/rspec")
            data.should == "different data"
            cstat,children = @zk.children("/zkruby")
            children.should include("rspec")
            @zk.delete("/zkruby/rspec",stat.version)
            @zk.exists?("/zkruby/rspec").should be_false
        end

        it "should accept -1 to delete any version" do
            path = @zk.create("/zkruby/rspec","someData",ZK::ACL_OPEN_UNSAFE,:ephemeral)
            @zk.delete("/zkruby/rspec",-1)
            @zk.exists?("/zkruby/rspec").should be_false
        end

        it "should raise ZooKeeperErrors in various circumstances"

        it "should call the error call back for asynchronous errors" do
            op = @zk.get("/an/unknown/path") do
                op.results = :callback_invoked_unexpectedly
            end

            op.on_error do |err|
                case err
                when ZK::Error::NO_NODE
                    op.results = :found_no_node_error
                else
                    op.results == err
                end
            end

            op.waitfor.should == :found_no_node_error
        end

        describe "anti herd-effect features" do
            it "should randomly shuffle the address list"

            it "should randomly delay reconnections within one seventh of the timeout"
        end    


        context "auto reconnect" do

            it "should stay connected" do
                #TODO: needs to be parameterized
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
                lambda { @zk.exists?("/zkruby") }.should raise_error(ZooKeeper::Error)
            end

        end


        context "mixed sync and async calls" do

            before(:each) do
              @zk.rmpath("/zkruby/sync_async")
              #@zk.delete("/zkruby/sync_async", -1) if @zk.exists?("/zkruby/sync_async") 
            end

            it "should handle a synchronous call inside an asynchronous callback" do
                op = @zk.create("/zkruby/sync_async","somedata",ZK::ACL_OPEN_UNSAFE) do
                    begin
                        stat, data = @zk.get("/zkruby/sync_async")
                        puts "got data #{data}"
                        op.results = data
                    rescue Exception => ex
                        op.results = ex
                    end
                end

                op.on_error { |err| op.results = ZK::Error.lookup(err) }

                op.waitfor().should == "somedata"
            end

            it "should handle a synchronous call inside an asynchronous error callback" do
                op = @zk.create("/zkruby/some_never_created_node/test","test_data",ZK::ACL_OPEN_UNSAFE) do
                    op.results = :should_not_get_here
                end

                op.on_error do |err|
                    case err
                    when ZK::Error::NO_NODE
                        stat,data = @zk.get("/zkruby") 
                        op.results = :success
                    else
                        op.results = [:unexpected_error, err]
                    end
                end

                op.waitfor().should == :success
            end
        end
    end
end

