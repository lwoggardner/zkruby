shared_examples_for "watches" do
    describe "watches" do
        around(:each) do |example|
            @zk = connect(:binding => binding)
            @zk2 = connect(:binding => binding)

            example.run

            safe_close(@zk)
            safe_close(@zk2)
        end

        it "should handle data watches" do
            path = @zk.create("/zkruby/rspec_watch","somedata",ZK::ACL_OPEN_UNSAFE,:ephemeral,:sequential)
            watch_results = []
            watch = lambda { |state,path,event| watch_results << [ state,path,event ] }

            stat,data = @zk.get(path,watch)
            # set the data on the 2nd session
            @zk2.set(path,"newdata",stat.version)
            Strand.sleep(2)
            watch_results.size().should == 1
            watch_results[0][1].should == path
            watch_results[0][2].should === :node_data_changed
        end

        it "should handle exists watches"
        it "should handle child watches" do
            watch_results = []
            watch = lambda { |state,path,event| watch_results << [ state,path,event ] }

            stat,children = @zk.children("/zkruby",watch)
            path = @zk2.create("/zkruby/rspec_watch","somedata",ZK::ACL_OPEN_UNSAFE,:ephemeral,:sequential)
            Strand.sleep(2)
            watch_results.size().should == 1
            watch_results[0][1].should == "/zkruby"
            watch_results[0][2].should === :node_children_changed

        end

        it "should reset watches over socket disconnects"
        it "should not send :connection_lost to watches on disconnect if so configured"

        describe :close do
            it "should send :session_expired to the default watcher on close"
            it "should send :Session_expired to all watchers on close"
        end
    end
end
