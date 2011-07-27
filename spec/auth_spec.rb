require 'spec_helper'

describe "ZooKeeper Authentication" do

    before(:all) do
        @path = "/zkruby/rspec-auth"
        @zk = connect(:scheme => "digest", :auth => "myuser:mypass")
        @zk.create(@path,"Auth Test",ZK::ACL_CREATOR_ALL,:ephemeral)
    end

    after(:all) do
        @zk.close()
    end
        
    it "should not allow unauthenticated access" do
        zk2 = connect()
        lambda { zk2.get(@path) }.should raise_error(ZooKeeperError)
        zk2.close()
   end

    it "should allow authenticated access" do
        zk2 = connect(:scheme => "digest", :auth => "myuser:mypass")
        stat,data = zk2.get(@path)
        data.should == "Auth Test"
        zk2.close()
    end

    it "should not allow access with bad password" do
        zk2 = connect(:scheme => "digest", :auth => "myuser:badpass")
        lambda { zk2.get(@path) }.should raise_error(ZooKeeperError)
        zk2.close()
    end

    # Digest doesn't actually validate credentials, just ensures they match against the ACL
    # We'll need to test this with a mock Binding
    it "should got to auth_failed if invalid credentials supplied" 

end
