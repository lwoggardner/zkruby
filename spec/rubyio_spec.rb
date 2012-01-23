require 'shared/binding'
require 'zk/rubyio'

describe ZooKeeper::RubyIO::Binding do
    it_behaves_like "a zookeeper client binding"
end

