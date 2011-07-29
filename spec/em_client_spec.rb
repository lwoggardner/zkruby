require 'spec_helper'
require 'eventmachine'
require 'zk/eventmachine'
require 'fiber'

module EMHelper
    def em_restart_cluster(delay=0)
        f = Fiber.current
        op = Proc.new { restart_cluster(delay) } 
        cb = Proc.new { |result| f.resume("done") }
        defer = EM.defer(op,cb) 
        #defer.errback() { f.resume("fail") }
        Fiber.yield.should == "done"
    end
end

include EMHelper
describe ZooKeeper::Client do
    describe "A event machine connection" do

        around(:each) do |example|
         ::EventMachine.run {
             f = Fiber.new() do
                puts (f)
                example.run
             end                                              
             f.resume
        }
        end

        before(:each) do
            @zk = connect(:timeout => 2.0, :binding => ZooKeeper::EventMachine::Binding )
            result =  @zk.exists("/zkruby")
            puts("Exists in before #{result.inspect}")
            unless result
                @zk.create("/zkruby","node for zk ruby testing",ZK::ACL_OPEN_UNSAFE)
            end
        end

        after(:each) do
            begin 
                @zk.close()
            rescue ZooKeeperError => ex
                puts("Error closing zk #{ex}")
            ensure
                EM::stop_event_loop()
            end
        end

        it "should return a stat for the root path" do
            stat = @zk.stat("/")
            stat.should be_a ZooKeeper::Data::Stat
        end

        it "should asynchronously return a stat for the root path" do
            f = Fiber.current
            op = @zk.stat("/") do |stat|
                stat.should be_a ZooKeeper::Data::Stat
                f.resume("done")
            end
            #op.errback { |err| f.resume(err) }
            puts (f) 
            Fiber.yield.should == "done"
        end

        it "should perform all the ZooKeeper CRUD" do
            path = @zk.create("/zkruby/rspec","someData",ZK::ACL_OPEN_UNSAFE,:ephemeral)
            path.should == "/zkruby/rspec"
            stat,data = @zk.get("/zkruby/rspec")
            stat.should be_a ZooKeeper::Data::Stat
            data.should == "someData"
            new_stat = @zk.set("/zkruby/rspec","different data",stat.version)
            new_stat.should be_a ZooKeeper::Data::Stat
            stat,data = @zk.get("/zkruby/rspec")
            data.should == "different data"
            @zk.delete("/zkruby/rspec",stat.version)
            @zk.exists?("/zkruby/rspec").should be_false
        end

        it "should stay connected" do
            f = Fiber.current
            EM.add_timer(@zk.timeout * 2.0) {
                op  = @zk.exists?("/zkruby") do |stat|
                    f.resume("done")
                end
                op.errback { f.resume ("failed") }
            }
            Fiber.yield.should == "done"
        end


        it "should seamlessly reconnect within the timeout period" do
            watcher = mock("Watcher").as_null_object
            watcher.should_receive(:process_watch).with(ZK::KeeperState::DISCONNECTED,nil,ZK::WatchEvent::NONE)
            watcher.should_receive(:process_watch).with(ZK::KeeperState::CONNECTED,nil,ZK::WatchEvent::NONE)
            watcher.should_not_receive(:process_watch).with(ZK::KeeperState::EXPIRED,nil,ZK::WatchEvent::NONE)
            @zk.watcher = watcher
            em_restart_cluster()
            @zk.exists("/zkruby").should be_true
        end
        
        it "should eventually expire the session" do
            watcher = mock("Watcher").as_null_object
            watcher.should_receive(:process_watch).with(ZK::KeeperState::DISCONNECTED,nil,ZK::WatchEvent::NONE)
            watcher.should_receive(:process_watch).with(ZK::KeeperState::EXPIRED,nil,ZK::WatchEvent::NONE)
            @zk.watcher = watcher
            em_restart_cluster(@zk.timeout * 2.0)
            lambda { puts "exists in lambda"; @zk.exists?("/zkruby") }.should raise_error(ZooKeeperError)
        end


    end
end
