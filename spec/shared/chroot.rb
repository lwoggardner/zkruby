require 'spec_helper'


shared_examples_for "chrooted connection" do
    describe "chrooted connection" do

        around(:each) do |example|
            @zk = connect(:binding => binding)
            @ch_zk = connect(:binding => binding, :chroot => "/zkruby")

            example.run

            safe_close(@zk)
            safe_close(@ch_zk)
        end

        it "should perform all the ZooKeeper CRUD chrooted" do
            path = @ch_zk.create("/spec-chroot","someData",ZK::ACL_OPEN_UNSAFE,:ephemeral)
            path.should == "/spec-chroot"
            real_path = "/zkruby#{path}"
            stat,data = @ch_zk.get(path)
            stat.should be_a ZooKeeper::Data::Stat
            data.should == "someData"

            stat,data = @zk.get(real_path)
            data.should == "someData"

            new_stat = @ch_zk.set(path,"different data",stat.version)
            new_stat.should be_a ZooKeeper::Data::Stat

            stat,data = @ch_zk.get(path)
            data.should == "different data"

            stat,data = @zk.get(real_path)
            data.should == "different data"

            @ch_zk.delete(path,stat.version)
            @ch_zk.exists?(path).should be_false
        end

        it "should return a client path for sync" do
            path = @ch_zk.sync("/aPath")
            path.should == "/aPath"
        end

        context "util recipes" do

            it "Client#mkpath should work with chroot" do
                @zk.rmpath("/zkruby/spec-util") 
                @ch_zk.mkpath("/spec-util/test/mkpath")
                stat,data = @zk.get("/zkruby/spec-util/test")
                data.should == ""
            end

            it "Client#rmpath should work with chroot" do
                @zk.mkpath("/zkruby/spec-util/test")
                @zk.mkpath("/zkruby/spec-util/test/two/three/four")
                @ch_zk.rmpath("/spec-util")
                @zk.exists?("/zkruby/spec-util").should be_false
            end

        end
    end
end
