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
                @session.start()
            end

            def connect(host,port,delay,timeout)
                EM.add_timer(delay) do
                    EM.connect(host,port,ZooKeeper::EventMachine::ClientConn,@session,timeout)
                end
            end

            # You are working in event machine it is up to you to ensure your callbacks do not block
            def invoke(callback,*args)
                callback.call(*args)
            end

            def queue_request(*args,&callback)
                op = AsyncOp.new(self,&callback)
                begin
                    @session.queue_request(*args) do |error,response|
                        op.resume(error,response)
                    end
                rescue ZooKeeper::Error => ex
                    op.resume(ex,nil)
                end

                op
            end

            def close(&callback)

                op = AsyncOp.new(self,&callback)

                begin
                    @session.close() do |error,response|
                        op.resume(error,response) 
                    end
                rescue ZooKeeper::Error => ex
                    op.resume(ex,nil)
                end

                op
            end

        end #class Binding

        class AsyncOp < ZooKeeper::AsyncOp

            def initialize(binding,&callback)
                super()
                @em_binding = binding

                # Wrap the callback in its own Strand
                @op_strand = Strand.new do
                    #immediately pause
                    error, response = Strand.yield
                    Strand.current[ZooKeeper::CURRENT] =  [ @em_binding.client ] 
                    raise error if error
                    callback.call(response)
                end
            end

            def resume(error,response)
                #TODO - raise issue in strand for resume to take arguments
                op_strand.fiber.resume(error,response)
            end

            private

            attr_reader :op_strand,:err_strand

            def set_error_handler(errcallback)
                @err_strand = Strand.new() do
                    begin
                        op_strand.value()
                    rescue StandardError => ex
                        Strand.current[ZooKeeper::CURRENT] =  [ @em_binding.client ] 
                        errcallback.call(ex)   
                    end
                end
            end

            def wait_value()
                err_strand ? err_strand.value : op_strand.value 
            end

        end #class AsyncOp
    end #module EventMachine
end #module ZooKeeper

ZooKeeper::BINDINGS << ZooKeeper::EventMachine::Binding
