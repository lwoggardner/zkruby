require 'server_helper'
require 'shared/binding'
require 'zkruby/eventmachine'

describe ZooKeeper::EventMachine::Binding do

    include Slf4r::Logger

    around(:each) do |example|
        EventMachine.run {
            Strand.new() do
            begin
                example.run
            rescue Exception => ex
                logger.error("Exception in example",ex)
            ensure
                EM::stop
            end
            end
        }
    end

    it "should be running in event machine" do
        Strand.event_machine?.should be_true
    end

    it_should_behave_like "a zookeeper client binding"

end
