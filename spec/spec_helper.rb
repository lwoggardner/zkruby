
require 'slf4r/logging_logger'
require 'zkruby'
require 'zk/rubyio'

Logging.logger.root.level = :error
Logging.logger.root.appenders = Logging.appenders.stdout(:layout => Logging.layouts.pattern(:pattern => '%c [%T] %-5l: %m\n'))
Logging.logger[ZooKeeper::RubyIO::Connection].level = :error
Logging.logger[ZooKeeper::RubyIO::Binding].level = :error
Logging.logger[ZooKeeper::Session].level = :error
Logging.logger["ZooKeeper::Session::Ping"].level = :error

Thread.current[:name] = "Rspec::Main"
module ZooKeeperSpecHelper

    include Slf4r::Logger

    def restart_cluster(delay=0)
        system("../../bin/zkServer.sh stop >> zk.out")
        Kernel::sleep(delay) if delay > 0
        if (::RUBY_PLATFORM == "java")
            #in JRuby 1.6.3 system does not return 
            system("../../bin/zkServer.sh start >> zk.out &")
        else
            system("../../bin/zkServer.sh start >> zk.out")
        end

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

include ZooKeeperSpecHelper

