require 'eventmachine'

if defined?(JRUBY_VERSION) && JRUBY_VERSION =~ /1\.6\.5.*/
    raise "Fibers are broken in JRuby 1.6.5 (See JRUBY-6170)"
end

require 'strand'

module ZooKeeper
    module EventMachine

        class ClientConn < ::EM::Connection
            include Protocol
            include Slf4r::Logger        

            unless EventMachine.methods.include?(:set_pending_connect_timeout)
                def set_pending_connect_timeout(timeout)

                end
            end  

            def initialize(session,connect_timeout)
                @session = session
                @connect_timeout = connect_timeout
                set_pending_connect_timeout(connect_timeout)
            rescue Exception => ex
                logger.warn("Exception in initialize",ex)
            end

            def post_init()
            rescue Exception => ex
                logger.warn("Exception in post_init",ex)
            end

            def connection_completed()
                @session.prime_connection(self)

                # Make sure we connect within the timeout period
                # TODO this should really be the amount of connect timeout left over
                @timer = EM.add_timer(@connect_timeout) do
                    if @session.connected?
                        # Start the ping timer 
                        ping = @session.ping_interval 
                        @timer = EM.add_periodic_timer ( ping ) do
                            case @ping
                            when 1 then @session.ping()
                            when 2 then close_connection()
                            end
                            @ping += 1
                        end
                    else
                        close_connection()
                    end
                end

            rescue Exception => ex
                logger.warn("Exception in connection_completed",ex)
            end

            def receive_records(packet_io)
                @ping = 0
                @session.receive_records(packet_io)
            end

            def disconnect()
                close_connection()
            end

            def unbind
                EM.cancel_timer(@timer) if @timer
                @session.disconnected()
            rescue Exception => ex
                logger.warn("Exception in unbind",ex)
            end

        end


        # The EventMachine binding is very simple because there is only one thread!
        # and we have good stuff like timers provided for us
        class Binding
            include Slf4r::Logger        
            # We can use this binding if we are running in the reactor thread
            def self.available?()
                EM.reactor_running? && EM.reactor_thread?
            end

            def self.context(&context_block)
                s = Strand.new() do
                    context_block.call(Strand)
                end
                s.join
            end

            attr_reader :client, :session
            def start(client,session)
                @client = client
                @session = session

                @event_strand = Strand.new do
                   Strand.current[ZooKeeper::CURRENT] = [ @client ]
                   loop do
                       break unless pop_event_queue
                   end
                   logger.debug { "Event strand finished"}
                end
                @session.start()
            end

            def pop_event_queue()
                #TODO - use Strand.yield when Strand is updated
                queued = Fiber.yield
                return false unless queued
                callback,*args = queued
                callback.call(*args)
                return true
            rescue Exception => ex
                logger.error("Exception in event strand", ex)
                #TODO - should this be raised? 
            end

            def event_strand?()
                Strand.current.equal?(@event_strand)
            end

            def connect(host,port,delay,timeout)
                EM.add_timer(delay) do
                    EM.connect(host,port,ZooKeeper::EventMachine::ClientConn,@session,timeout)
                end
            end

            # You are working in event machine it is up to you to ensure your callbacks do not block
            def invoke(callback,*args)
                @event_strand.fiber.resume(callback,*args)
            end

            def queue_request(*args,&callback)
                 AsyncOp.new(self,callback) do |op|
                    @session.queue_request(*args) do |error,response|
                        op.resume(error,response)
                    end
                end
            end

            def close(&callback)
                AsyncOp.new(self,callback) do |op|
                    @session.close() do |error,response|
                        op.resume(error,response) 
                    end
                end
            end

        end #class Binding

        class AsyncOp < ZooKeeper::AsyncOp

            def initialize(binding,callback,&operation)
                @em_binding = binding
                @cv = Strand::ConditionVariable.new()
                super(callback,&operation)
            end
 
            private

            attr_reader :cv,:error,:result
            
            def process_resume(error,response)
                @error,@result = process_response(error,response)
                cv.signal() if resumed?
            end

            def wait_value()
                if @em_binding.event_strand?
                    until resumed?
                        break unless @em_binding.pop_event_queue()
                    end

                    #TODO there's a problem if we have not been resumed
                    #and the event strand it dead.
                    logger.error { "Not resumed and event strand is dead" } unless resumed?
                else
                    cv.wait() unless resumed?
                end

                raise error if error
                result
            end

        end #class AsyncOp
    end #module EventMachine
end #module ZooKeeper

ZooKeeper.add_binding(ZooKeeper::EventMachine::Binding)
