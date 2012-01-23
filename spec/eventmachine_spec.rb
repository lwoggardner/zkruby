require 'shared/binding'
require 'zk/eventmachine'

module EMHelper
    alias :restart_cluster_orig :restart_cluster
    def restart_cluster(delay=0)
        if EM.reactor_running?
                cv = Strand::ConditionVariable.new()
                op = Proc.new do
                    begin
                        restart_cluster_orig(delay)
                    rescue Exception => ex
                        logger.error ("Exception restarting cluster #{ex}")
                    end
                    true
                end
                cb = Proc.new { |result| cv.signal() } 
                defer = EM.defer(op,cb)
                cv.wait()
        else
            restart_cluster_orig(delay)
        end
    end

    def sleep(delay)
        Strand.sleep(delay)
    end
end

describe ZooKeeper::EventMachine::Binding do

    include Slf4r::Logger
    include EMHelper

    around(:each) do |example|
        EventMachine.run {
            Fiber.new() do
               begin
                example.run
               ensure
                EM::stop
               end
            end.resume
        }
    end

    it_should_behave_like "a zookeeper client binding"

end
