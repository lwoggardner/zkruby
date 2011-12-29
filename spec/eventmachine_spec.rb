require 'shared/client_binding'
require 'eventmachine'
require 'zk/eventmachine'
require 'fiber'

module EMHelper
    alias :restart_cluster_orig :restart_cluster
    def restart_cluster(delay=0)
        if EM.reactor_running?
            f = Fiber.current
            op = Proc.new do
                begin
                    restart_cluster_orig(delay)
                rescue Exception => ex
                    logger.error ("Exception restarting cluster #{ex}")
                end
                true
            end
            cb = Proc.new { |result| f.resume("done") }
            defer = EM.defer(op,cb)
            Fiber.yield.should == "done"
        else
            restart_cluster_orig(delay)
        end
    end
    
    def sleep(delay)
        f = Fiber.current
        EM.add_timer(delay) {
            f.resume("done")
        }
        Fiber.yield.should == "done"
    end
end

describe ZooKeeper::EventMachine::Binding do

    include Slf4r::Logger
    include EMHelper

    around(:each) do |example|
        ::EventMachine.run {
            f = Fiber.new() do
            example.run
            end                                              
        f.resume
        }
    end

    after(:each) do
        EM::stop_event_loop()
    end

    it_should_behave_like "a zookeeper client binding"

end
