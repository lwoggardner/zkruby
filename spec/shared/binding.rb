require 'spec_helper'
require 'shared/basic'
require 'shared/util'
require 'shared/chroot'
require 'shared/watch'
require 'shared/multi'
require 'shared/auth'

shared_examples_for "a zookeeper client binding" do

    let (:binding) { described_class }

    context "A local connection" do
    
        before(:all) do
            restart_cluster(2)
        end

        around(:each) do |example|
            ZooKeeper.connect(get_addresses(),:binding => binding) do | zk |
                @zk = zk
                ZooKeeper.current.should == zk
                example.run
            end
        end

        include_examples("basic integration")
        include_examples("util recipes")
        include_examples("multi")

    end

    include_examples("authentication")
    include_examples("chrooted connection")
    include_examples("watches")
end

