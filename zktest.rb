require 'zkruby'
#require 'zk/eventmachine'
require 'zk/rubyio'

module ZooKeeper
  extend ZooKeeper::Logger
  module EventMachine
    class Binding
        def self.test(addresses,options)
            ::EventMachine.run {
               f = Fiber.new() do
                    ZooKeeper.zktest(addresses,options) { ::EM.stop_event_loop() }
               end
               f.resume
            }
        end
    end
  end

  module RubyIO
      class Binding
        def self.test(addresses,options)
            ZooKeeper.zktest(addresses,options)
            puts("Finished test")
            sleep(20)
        end
      end
  end

    def self.zktest(addresses,options,&close_block)
        zk = ZooKeeper::connect(addresses,options)
        
        begin
           #stat,children = zk.children("/")
           #puts children.join(" ")
           watch = lambda { |state,event,path| puts "A Watch #{state} #{event} #{path}" }
           #unless zk.exists?("/hello999",watch)
               #stat,data = zk.get("/hello999")
               #puts "Syncget / #{stat.inspect}, data=#{data}"
           #    acl = ZooKeeper::ACL_OPEN_UNSAFE
           #    path = zk.create("/hello999","Hi There!",acl,:ephemeral)
           #    puts "Created #{path}"
           #end
           stat = zk.stat("/hello999")
           puts "#{stat.inspect}\n"

           zk.exists("/hello999") do | stat |
             puts "exists(/hello999)=#{stat}\n"
           end
        rescue ZooKeeperError => ex
           log.warn("Exception in test ",ex)
        ensure
           zk.close() do
                puts ("Closed zktest")
                close_block.call if block_given?
           end
        end
        
    end
end

addresses="localhost:2181"

bindings = ZooKeeper::BINDINGS

bindings.each do | binding |
    options = {:binding => binding }
    binding.test(addresses,options)
end

    # set the global watcher, use this to respond to session events
    #zk.watcher do | zk_state, event, path |
    #    case zk_state
    #    when :expired
    #        EventMachine.stop_event_loop()
    #    end
    #end

    #watcher = lambda { | state,event,path | puts "#{state} #{event} #{path}" }
    #op = zk.get("hello",watcher) do |stat,data|
           
    #end

    #stat,children = zk.children("/",true)


    # true -> register the global watcher for the path/op
    # Proc , better have arity of 3
    # respond_to?(:process_watch) with arity of 3
    #Do something everytime a node is changed until session disconnects
    #zk.each_update("/hello") do
       #Fiber as generator - in a recipe not part of api
    #end

    

    #zk.get("/") do |stat,data|
    #    puts "Asyncget #{stat.inspect}, data=#{data}"
    #end

    #begin
    #    stat,data =  zk.get("/asd")
    #rescue ZooKeeperError => ex
    #    puts("Caught exception: #{ex.message}")
    #end
   
    #get = zk.get("/bdjs") do | stat, data|
    #    puts "\n\nResult 2#{stat.inspect}: data=#{data}"
    #end

    #get.on_error { |err| puts "got err #{err} getting /bdjs" }


