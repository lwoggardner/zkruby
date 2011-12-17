require 'recipe_helper'
require 'recipes/node.rb'

describe ZooKeeper::NodeWatcher do
    def send_watch(state,path,event)
        @watcher.process_watch(ZK::KeeperState.get(state), path,
                               ZK::WatchEvent.get(event))
    end

    before(:each) do
        @zkd = double("zk double")
        @zk = ZooKeeper::MockClient.new(@zkd)
        @path = "/some/path"
        @notifier = double("receiver")
        @mockstat = double("a stat")
        @watcher = ZK::NodeWatcher.new(@zk,@path,@notifier)
    end

    after(:each) do
        @watcher.run()
        @zk.run_queue()
    end

    it "should emit data events for each child of a path" do

        @zkd.stub(:children).with(@path,@watcher).and_return([@mockstat,["a","b","c"]])
        @zkd.stub(:get).and_return([@mockstat,"dataA"],[@mockstat,"dataB"],[@mockstat,"dataC"])
        @zkd.stub(:get).with(@path).and_return([@mockstat,"root"]) 

        @notifier.should_receive(:call).with("#{@path}/a",@mockstat,"dataA")
        @notifier.should_receive(:call).with("#{@path}/b",@mockstat,"dataB")
        @notifier.should_receive(:call).with("#{@path}/c",@mockstat,"dataC")
        @notifier.should_receive(:call).with(@path,@mockstat,"root")

    end

    context "watches trigger updates"  do
        
        before(:each) do
            @zkd.stub(:children).with(@path,@watcher).and_return([@mockstat,["a","b","c"]])
            @zkd.stub(:get).and_return([@mockstat,"dataA"],[@mockstat,"dataB"],[@mockstat,"dataC"])
            @zkd.stub(:get).with(@path).and_return([@mockstat,"root"])
            @notifier.stub(:call)
            @watcher.run()
            @zk.run_queue()
        end

        it "should emit data events for new nodes that are added" do
            @zkd.stub(:children).with(@path,@watcher).and_return([@mockstat,["a","b","c","d"]])
            @zkd.stub(:get).with("#{@path}/d",@watcher).and_return([@mockstat,"dataD"])
            
            @notifier.should_receive(:call).with("#{@path}/d",@mockstat,"dataD")
            send_watch(:connected,@path,:node_children_changed) 
        end

        it "should emit events for nodes that are deleted" do
            @zkd.stub(:children).with(@path,@watcher).and_return([@mockstat,["a","b"]])
            
            @notifier.should_receive(:call).with("#{@path}/c",nil,nil)
        
            send_watch(:connected,@path,:node_children_changed)
        end

        it "should emit events for nodes that are updated" do
            @zkd.stub(:get).with("#{@path}/b",@watcher).and_return([@mockstat,"updatedB"])
        
            send_watch(:connected,"#{@path}/b",:node_data_changed)
            @notifier.should_receive(:call).with("#{@path}/b",@mockstat,"updatedB")
            
        end
    end

    context "recovery after client disconnected" do
        before(:each) do
            
            @notifier.should_receive(:call).with("#{@path}/a",@mockstat,"dataA").ordered
            @notifier.should_receive(:call).with("#{@path}/b",@mockstat,"dataB")
            @notifier.should_receive(:call).with("#{@path}/c",@mockstat,"dataC")
            # It is important that the root node is emitted last, so we setup
            # an ordered expectation to test that
            @notifier.should_receive(:call).with(@path,@mockstat,"root").ordered
        end

        it "should retry on disconnect during fetch of children list" do
            count = 0
            @zkd.stub(:children).with(@path,@watcher) {
                count = count + 1 
                raise ZooKeeper::Error.lookup(:connection_lost) if count == 1
                [@mockstat,["a","b","c"]]
            }
            @zkd.stub(:get).and_return([@mockstat,"dataA"],[@mockstat,"dataB"],[@mockstat,"dataC"])
            @zkd.stub(:get).with(@path).and_return([@mockstat,"root"]) 
        end

        it "should retry on disconnect during fetch of child data" do
            @zkd.stub(:children).with(@path,@watcher).and_return([@mockstat,["a","b","c"]])
            count = 0
            data = { "#{@path}/a" => "dataA",
                     "#{@path}/b" => "dataB",
                     "#{@path}/c" => "dataC"}
            @zkd.stub(:get) { |path|
                count += 1
                raise ZooKeeper::Error.lookup(:connection_lost) if count == 2
                [@mockstat,data[path]]
            }
            @zkd.stub(:get).with(@path).and_return([@mockstat,"root"]) 
        end

        it "should retry on disconnect during final fetch of root node" do
            @zkd.stub(:children).with(@path,@watcher).and_return([@mockstat,["a","b","c"]])
            @zkd.stub(:get).and_return([@mockstat,"dataA"],[@mockstat,"dataB"],[@mockstat,"dataC"])
            count = 0
            @zkd.stub(:get).with(@path) {
                count += 1
                raise ZooKeeper::Error.lookup(:connection_lost) if count == 1
                [@mockstat,"root"] 
            }
        end

    end

    context "watches firing while fetching child data" do
        # if we just fetch as watches fire then it is possible we will never
        # fire the watch root marker event because some other node is rapidly updating
        it "should fire watch node emit before emiting new child data" do
            @zkd.stub(:children).with(@path,@watcher).and_return(
                [@mockstat,["a","b","c"]],[@mockstat,["a","b","c","d"]]
            )

            count = 0
            data = { "#{@path}/a" => "dataA",
                     "#{@path}/b" => "dataB",
                     "#{@path}/c" => "dataC",
                     "#{@path}/d" => "dataD",
                     @path => "root" }

            @zkd.stub(:get) { |path|
                count += 1
                send_watch(:connected, "#{@path}/c",:node_data_changed) if count == 2
                send_watch(:connected, @path,:node_children_changed) if count == 3
               
                #otherwise we return the current data, and update it for next time round
                data_content = data[path]
                data[path] = data_content + "updated"
                [@mockstat,data_content]
            }

            @notifier.should_receive(:call).with("#{@path}/a",@mockstat,"dataA")
            @notifier.should_receive(:call).with("#{@path}/b",@mockstat,"dataB")
            @notifier.should_receive(:call).with("#{@path}/c",@mockstat,"dataC").ordered
            @notifier.should_receive(:call).with(@path,@mockstat,"root").ordered
            @notifier.should_receive(:call).with("#{@path}/c",@mockstat,"dataCupdated").ordered
            @notifier.should_receive(:call).with("#{@path}/d",@mockstat,"dataD")
            @notifier.should_receive(:call).with(@path,@mockstat,"rootupdated").ordered
        end
    end
end
