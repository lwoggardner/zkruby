require 'spec_helper'

module ZooKeeperServerHelper

    include Slf4r::Logger

    def restart_cluster(delay=0)
        system("../../bin/zkServer.sh stop >> zk.out")
        Kernel::sleep(delay) if delay > 0
        if (::RUBY_PLATFORM == "java" && Gem::Version.new(JRUBY_VERSION) < Gem::Version.new("1.7.0"))
            #in JRuby 1.6.3 system does not return 
            system("../../bin/zkServer.sh start >> zk.out &")
        else
            system("../../bin/zkServer.sh start >> zk.out")
        end
    end

    def get_addresses()
        "localhost:2181"
    end

    def safe_close(zk)
        zk.close()
        rescue ZooKeeper::Error => ex
            puts "Ignoring close exception #{ex}"
    end

    def connect(options = {})
        ZooKeeper.connect("localhost:2181",options)
    end

end

include ZooKeeperServerHelper

restart_cluster()
sleep(3)
require 'net/telnet'
t = Net::Telnet.new("Host" => "localhost", "Port" => 2181)
properties = t.cmd("mntr")

RSpec.configure do |c|
    #Exclude multi unless we are on a 3.4 server
    c.filter_run_excluding :multi => true unless properties
    c.filter_run_excluding :perf => true
end
