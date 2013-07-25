require 'server_helper'
require 'shared/binding'

describe ZooKeeperBinding do
    let (:pass_every) { 100 }
    it_behaves_like "a zookeeper client binding"
end

