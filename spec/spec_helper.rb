require 'slf4r/ruby_logger'
Slf4r::LoggerFacade4RubyLogger.level = :info

require 'zkruby'
require 'zk/rubyio'
module ZooKeeperSpecHelper

    def restart_cluster(delay=0)
        system("../../bin/zkServer.sh stop >> zk.out")
        sleep(delay) if delay > 0
        system("../../bin/zkServer.sh start >> zk.out")
    end

    def connect(options = {})
        ZooKeeper.connect("localhost:2181",options)
    end

end

include ZooKeeperSpecHelper
