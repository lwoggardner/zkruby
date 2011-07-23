= zkruby

* https://github.com/lwoggardner/zkruby

== DESCRIPTION:

Pure ruby client for ZooKeeper

== FEATURES/PROBLEMS:

*  JRuby does not need a different binding - works over eventmachine or rubyio

== SYNOPSIS:

  require 'zkruby'
  require 'zk/rubyio'
    
  zk = ZooKeeper.connect("localhost:2181")
  # Synchronous
  stat = zk.exists("/aPath")

  # Asynchronous
  zk.exists("/aPath) { |stat| puts stat.inspect }

  # Watches
  watch = lambda { |state,event,path| puts "Watch fired #{state} #{event} #{path}" }

  zk.exists("/aPath",watch)

  # OR with EventMachine
  require 'eventmachine'
  require 'zk/eventmachine'

  EventMachine.run do
     # Magic of Fibers lets us code synchronous while really executing asynchronous
     f = Fiber.new() do
        begin
            zk = ZooKeeper.connect("localhost:2181")
            #Sync
            path = zk.create("/aPath/mynode",ZK::ACL_ANyONE_UNSAFE,:ephemeral,:sequential) 

            #Async
            zk.get(path) do |stat,data|
               puts "#{stat.inspect} #{data}" 
            end
            
        rescue ZooKeeperException => zkex
            puts zkex.message
        end

     end
     f.resume()
  end

== REQUIREMENTS:

* A ZooKeeper cluster to connect to
* Ruby 1.9 

== INSTALL:

* FIX (sudo gem install, anything else)

== DEVELOPERS:

After checking out the source, run:

  $ rake newb

This task will install any missing dependencies, run the tests/specs,
and generate the RDoc.

== LICENSE:

(The MIT License)

Copyright (c) 2011 

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
