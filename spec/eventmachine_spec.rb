require 'server_helper'
require 'zkruby/eventmachine'
require 'shared/binding'

Empathy.empathise(ZooKeeper)

describe Empathy::EM::ZooKeeperBinding do

    include Slf4r::Logger

    around(:each) do |example|
        Empathy.run { example.run }
    end

    it "should be running in event machine" do
        Empathy.event_machine?.should be_true
    end

    let (:pass_every) { 3 }
    it_should_behave_like "a zookeeper client binding"
end
