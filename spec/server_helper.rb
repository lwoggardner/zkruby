require 'spec_helper'

module ZooKeeperServerManager

    include Slf4r::Logger

    JRUBY_COMPAT_SYSTEM = (RUBY_PLATFORM == "java" && Gem::Version.new(JRUBY_VERSION.dup) < Gem::Version.new("1.6.5"))

    def jruby_safe_system(arg)
        arg = "#{arg} &" if JRUBY_COMPAT_SYSTEM
        system(arg)
        sleep(3) if JRUBY_COMPAT_SYSTEM
    end

    def restart_cluster(delay=0)
        jruby_safe_system("../../bin/zkServer.sh stop >> zk.out")
        sleep(delay) if delay > 0
        jruby_safe_system("../../bin/zkServer.sh start >> zk.out")
    end

    def get_addresses()
        "127.0.0.1:2181"
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

include ZooKeeperServerManager

restart_cluster()
sleep(3)
require 'net/telnet'
t = Net::Telnet.new("Host" => "localhost", "Port" => 2181)
properties = t.cmd("mntr")

RSpec.configure do |c|
    #Exclude multi unless we are on a 3.4 server
    c.filter_run_excluding :multi => true unless properties
    #c.filter_run_excluding :perf => true
end
