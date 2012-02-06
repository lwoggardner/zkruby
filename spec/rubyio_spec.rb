require 'server_helper'
require 'shared/binding'
require 'zkruby/rubyio'

describe ZooKeeper::RubyIO::Binding do
    it_behaves_like "a zookeeper client binding"
end

