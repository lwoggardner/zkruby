# This is the main require for the eventmachine based binding
# Only use this if all use of zkruby will be within the EM Reactor 
require 'zkruby/zkruby'
require 'zkruby/eventmachine'

Empathy::EM.empathise(ZooKeeper)
