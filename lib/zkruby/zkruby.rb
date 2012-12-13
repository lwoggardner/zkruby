# A pure ruby implementation of the zk client library
#
# It implements the client side of the ZooKeeper TCP protocol directly rather
# than calling the zk client libraries
#

require 'zkruby/version'

module ZooKeeper
    @bindings = []
    def self.add_binding(binding)
        @bindings << binding unless @bindings.include?(binding)
    end
end

# Shorthand
ZK=ZooKeeper

require 'slf4r'
require 'zkruby/enum'
require 'zkruby/bindata'
require 'jute/zookeeper'
require 'zkruby/multi'
require 'zkruby/protocol'
require 'zkruby/session'
require 'zkruby/client'
# Utilities
require 'zkruby/util'
