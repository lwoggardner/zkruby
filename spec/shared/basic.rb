

shared_examples_for "basic integration" do

    before(:each) do
         @zk.create("/zkruby","node for zk ruby testing",ZK::ACL_OPEN_UNSAFE) unless @zk.exists?("/zkruby")
    end

    it "should return a stat for the root path" do
        stat = @zk.stat("/")
        stat.should be_a ZooKeeper::Data::Stat
    end

    it "should asynchronously return a stat for the root path" do
        op = @zk.stat("/") do |stat|
            stat.should be_a ZooKeeper::Data::Stat
            :success
        end

        op.value.should == :success
    end

    it "should perform ZooKeeper CRUD" do
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

    context "exceptions" do

        it "should raise error for synchronous method" do
            lambda { @zk.get("/anunknownpath") }.should raise_error(ZooKeeper::Error::NO_NODE)
        end

        it "should capture exceptions from asynchronous callback" do
            op = @zk.exists?("/zkruby") do |stat|
                raise "oops"
            end

            lambda { op.value }.should raise_error(RuntimeError)
        end


        it "should call the error call back for asynchronous errors" do
            op = @zk.get("/an/unknown/path") do
                :callback_invoked_unexpectedly
            end

            op.on_error do |err|
                puts "In error callback"
                case err
                when ZK::Error::NO_NODE
                    :found_no_node_error
                else
                    raise err
                end
            end

            op.value.should == :found_no_node_error
        end
    end

    context "anti herd-effect features" do
        it "should randomly shuffle the address list"

        it "should randomly delay reconnections within one seventh of the timeout"
    end    


    context "auto reconnect" do

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
            lambda { @zk.exists?("/zkruby") }.should raise_error(ZooKeeper::Error)
        end

    end


    context "mixed sync and async calls" do

        before(:each) do
            @zk.delete("/zkruby/sync_async", -1) if @zk.exists?("/zkruby/sync_async") 
        end

        it "should handle a synchronous call inside an asynchronous callback" do
            op = @zk.create("/zkruby/sync_async","somedata",ZK::ACL_OPEN_UNSAFE) do
                ZK.current.should equal(@zk)
                stat, data = @zk.get("/zkruby/sync_async")
                data
            end

            op.value.should == "somedata"
        end

        it "should handle a synchronous call inside an asynchronous error callback" do

            op = @zk.create("/zkruby/some_never_created_node/test","test_data",ZK::ACL_OPEN_UNSAFE) do
                :should_not_get_here
            end

            op.on_error do |err|
                ZK.current.should equal(@zk)
                case err
                when ZK::Error::NO_NODE
                    stat,data = @zk.get("/zkruby") 
                    :success
                else
                    raise err
                end
            end

            op.value.should == :success
        end
    end
end