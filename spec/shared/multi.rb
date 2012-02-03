
shared_examples_for "multi"  do

    context "only in 3.4 and up", :multi => true do
        it "should do a create in a multi-op" do
            delete_path = @zk.create("/zkruby/multi","hello multi",ZK::ACL_OPEN_UNSAFE,:ephemeral,:sequential)

            new_path = nil
            stat,data = @zk.get("/zkruby")
            @zk.transaction() do |txn|
                txn.create("/zkruby/multi","hello world", ZK::ACL_OPEN_UNSAFE,:ephemeral,:sequential) { |path| logger.debug{ "New path #{path}"} ; new_path = path }
                txn.check("/zkruby",stat.version)
                txn.set("/zkruby","new multi data",-1)
                txn.delete(delete_path,-1)
            end

            new_path.should_not be_nil

            stat, data = @zk.get(new_path)
            data.should == "hello world"

            stat, data = @zk.get("/zkruby")
            data.should == "new multi data"

            @zk.exists?(delete_path).should be_nil

        end

        it "should raise exceptions if the multi op fails" do
            lambda { @zk.transaction() do |txn|
                txn.create("/multi","hello world", ZK::ACL_OPEN_UNSAFE,:ephemeral,:sequential) { |path| puts path }
                txn.create("/multi/something/","hello world", ZK::ACL_OPEN_UNSAFE,:ephemeral,:sequential) { |path| puts path }
                txn.create("/multi","hello world 2", ZK::ACL_OPEN_UNSAFE,:ephemeral,:sequential) { |path| puts path }
            end }.should raise_error(ZooKeeper::Error)

        end
    end
end

