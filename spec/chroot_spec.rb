require 'spec_helper'

describe "chrooted connection" do
    before(:all) do
        @zk = connect()
        @ch_zk = connect(:chroot => "/zkruby")
    end

    after(:all) do
        @zk.close()
        @ch_zk.close()
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

end
