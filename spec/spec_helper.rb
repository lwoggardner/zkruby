require 'slf4r/logging_logger'
require 'zkruby/zkruby'

Logging.logger.root.level = :warn
Logging.logger.root.appenders = Logging.appenders.stdout(:layout => Logging.layouts.pattern(:pattern => '%r %c [%T] %-5l: %m\n'))
#Logging.logger[ZooKeeper::RubyIO::Connection].level = :error
#Logging.logger["ZooKeeper::RubyIO::Binding"].level = :debug
#Logging.logger[ZooKeeper::Session].level = :debug
#Logging.logger["ZooKeeper::EventMachine::ClientConn"].level = :debug
#Logging.logger["ZooKeeper::Session::Ping"].level = :error

Thread.current[:name] = "Rspec::Main"
