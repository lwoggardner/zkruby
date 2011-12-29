
require 'eventmachine'
if defined?(JRUBY_VERSION) && JRUBY_VERSION == "1.6.5"
    require 'jruby'
    org.jruby.ext.fiber.FiberExtLibrary.new.load(JRuby.runtime, false)
    class org::jruby::ext::fiber::ThreadFiber
        field_accessor :state
    end

    class Fiber
        def alive?
            JRuby.reference(self).state != org.jruby.ext.fiber.ThreadFiberState::FINISHED
        end
    end
else
    require 'fiber'
end


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

            attr_reader :session
            def start(session)
                @session = session
                @session.start()
            end

            def connect(host,port,delay,timeout)
                EM.add_timer(delay) do
                    EM.connect(host,port,ZooKeeper::EventMachine::ClientConn,@session,timeout)
                end
            end

            # You are working in event machine it is up to you to ensure your callbacks do not block
            def invoke(callback,*args)
                f = Fiber.new() do
                    callback.call(*args)
                end
                f.transfer()
            end

            def queue_request(*args,&blk)
                @session.queue_request(*args,&blk)
            end

            def async_op(packet)
                AsyncOp.new(packet)
            end

            def close(&blk)
                @session.close(&blk)
            end
        end

        class AsyncOp < ::ZooKeeper::AsyncOp

            def initialize(packet)
                super(packet)
                @fiber = Fiber.current
            end

            def results=(results)
                @fiber.resume(results)
            end

            private
            def wait_result()
                Fiber.yield()
            end

        end
    end #module ZooKeeper::EventMachine
end #module ZooKeeper

ZooKeeper::BINDINGS << ZooKeeper::EventMachine::Binding
