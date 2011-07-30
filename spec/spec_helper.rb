require 'slf4r/ruby_logger'
Slf4r::LoggerFacade4RubyLogger.level = :info

require 'zkruby'
require 'zk/rubyio'
module ZooKeeperSpecHelper

    def restart_cluster(delay=0)
        system("../../bin/zkServer.sh stop >> zk.out")
        sleep(delay) if delay > 0
        if (::RUBY_PLATFORM == "java")
            #in JRuby 1.6.3 system does not return 
            system("../../bin/zkServer.sh start >> zk.out &")
        else
            system("../../bin/zkServer.sh start >> zk.out")
        end

    end

    def safe_close(zk)
        zk.close()
        rescue ZooKeeperError => ex
            puts "Ignoring close exception #{ex}"
    end

    def connect(options = {})
        ZooKeeper.connect("localhost:2181",options)
    end

end

include ZooKeeperSpecHelper
