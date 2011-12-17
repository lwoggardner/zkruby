require 'spec_helper'
require 'recipes/util.rb'

describe "Client utilities" do

        before(:all) do
            restart_cluster(2)
            @zk = connect()
            unless @zk.exists("/zkruby")
                @zk.create("/zkruby","node for zk ruby testing",ZK::ACL_OPEN_UNSAFE)
            end
            @zk_ch = connect(:chroot => "/zkruby")
        end

        after(:all) do
            safe_close(@zk)
        end

        context "recursive make path" do
            #you'll note that these tests rely on each other!
            before(:each) do
                @zk.rmpath("/zkruby/spec-util")
            end

            it "should create empty nodes at all the intermediate paths" do
                @zk.mkpath("/zkruby/spec-util/test/mkpath")
                stat,data = @zk.get("/zkruby/spec-util/test")
                data.should == ""
                stat,data = @zk.get("/zkruby/spec-util/test/mkpath")
                data.should == ""
            end

            it "should not raise errors when path already exists" do
                @zk.mkpath("/zkruby")
            end

            it "should only create the final path if all parents exist" do
                @zk.create("/zkruby/spec-util","node that exists",ZK::ACL_OPEN_UNSAFE)
                @zk.mkpath("/zkruby/spec-util/mkpath")
                stat, data = @zk.get("/zkruby/spec-util")
                data.should == "node that exists"
            end

            it "should not raise error if something else creates an intermediate path"

            it "should work with chroot" do
                @zk_ch.mkpath("/spec-util/test/mkpath")
                stat,data = @zk_ch.get("/spec-util/test")
                data.should == ""
            end

        end

        context "Client#rmpath" do
            before(:each) do
                @zk.mkpath("/zkruby/spec_util/one")
                @zk.mkpath("/zkruby/spec_util/two/twopointtwo")
                @zk.mkpath("/zkruby/spec_util/two/three/four/five")
            end
              
            it "should delete a tree of paths" do
                @zk.rmpath("/zkruby/spec_util")
                @zk.exists?("/zkruby/spec_util").should be_false
            end

            it "should not raise errors if the path is already deleted"
            it "should delete leaf nodes just like delete does" do
                @zk.rmpath("/zkruby/spec_util/one")
                @zk.exists?("/zkruby/spec_util/one").should be_false
            end
           
            
            it "should not raise errors if some nodes are deleted by something else during processing"
            it "should fight to the death if something else is creating subnodes"

            it "should work with chroot" do
                @zk_ch.rmpath("/spec-util")
                @zk_ch.exists?("/spec-util").should be_false
            end
        end
end
