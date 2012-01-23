require 'spec_helper'
require 'shared/basic'
require 'shared/util'
require 'shared/chroot'

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

    end

    include_examples("chrooted connection")
end

