shared_examples_for "authentication" do

    describe "Authentication" do

        around(:each) do |example|
            @path = "/zkruby/rspec-auth"
            @zk = connect(:scheme => "digest", :auth => "myuser:mypass", :binding => binding)
            @zk.create(@path,"Auth Test",ZK::ACL_CREATOR_ALL,:ephemeral)

            example.run

            safe_close(@zk)
        end


        it "should not allow unauthenticated access" do
            zk2 = connect(:binding => binding)
            lambda { zk2.get(@path) }.should raise_error(ZooKeeper::Error)
            zk2.close()
        end

        it "should allow authenticated access" do
            zk2 = connect(:scheme => "digest", :auth => "myuser:mypass", :binding => binding)
            stat,data = zk2.get(@path)
            data.should == "Auth Test"
            zk2.close()
        end

        it "should not allow access with bad password" do
            zk2 = connect(:scheme => "digest", :auth => "myuser:badpass", :binding => binding)
            lambda { zk2.get(@path) }.should raise_error(ZooKeeper::Error)
            zk2.close()
        end

        # Digest doesn't actually validate credentials, just ensures they match against the ACL
        # We'll need to test this with a mock Binding
        it "should get to auth_failed if invalid credentials supplied" 

    end
end
