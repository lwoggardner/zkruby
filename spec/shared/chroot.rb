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

        context "multi", :multi => true do

            it "should do chrooted operations in a transaction" do
                new_path = nil

                # we're gonna delete this one
                delete_path = @zk.create("/zkruby/multi","hello multi",ZK::ACL_OPEN_UNSAFE,:ephemeral,:sequential)
                
                # we use the version in the check command below
                stat, data = @zk.get("/zkruby")
                @ch_zk.transaction() do |txn|
                    txn.create("/multi","chroot world", ZK::ACL_OPEN_UNSAFE,:ephemeral,:sequential) { |path| logger.debug{ "New path #{path}"} ; new_path = path }
                    txn.check("/",stat.version)
                    txn.set("/","chroot multi data",-1)
                    txn.delete(delete_path[7..-1],-1)
                end
                new_path.should_not be_nil

                stat, data = @zk.get("/zkruby#{new_path}")
                data.should == "chroot world"

                stat, data = @zk.get("/zkruby")
                data.should == "chroot multi data"

                @zk.exists?(delete_path).should be_nil

            end

        end
    end
end
