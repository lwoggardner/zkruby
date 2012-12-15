require 'eventmachine'

if defined?(JRUBY_VERSION) && JRUBY_VERSION =~ /1\.6\.5.*/
    raise "Fibers are broken in JRuby 1.6.5 (See JRUBY-6170)"
end

# Tell Strand that we want to consider event machine
Strand.reload()

module ZooKeeper
    module EventMachine

        class ClientConn < ::EM::Connection
            include Protocol
            include Slf4r::Logger        

            attr_reader :session
            unless EventMachine.methods.include?(:set_pending_connect_timeout)
                def set_pending_connect_timeout(timeout)

                end
            end  

            def initialize(session,connect_timeout)
                @fiber = Fiber.current
                @session = session
                @connect_timeout = connect_timeout
                set_pending_connect_timeout(connect_timeout)
            rescue Exception => ex
                logger.warn("Exception in initialize",ex)
            end

            def read_loop()
                event,*args = Fiber.yield
                if (event == :connection_completed)
                    logger.debug("Connection completed")
                    session.prime_connection(self)
                    
                    @timer = EM.add_timer(@connect_timeout) do
                        @fiber.resume(:connect_timer)
                    end

                    ping = 0
                    # If session sleeps or waits in here then our yield/resumes are going to get out of sync
                    while true
                        event,*args = Fiber.yield
                        case event
                        when :connect_timer
                            if session.connected?
                                @timer = EM.add_periodic_timer(session.ping_interval) do
                                    @fiber.resume(:ping_timer)
                                end
                            else
                                close_connection()
                            end
                        when :ping_timer
                            case ping
                            when 1 then session.ping
                            when 2 then close_connection
                            end
                            ping += 1
                        when :receive_records
                            ping = 0
                            packet_io = args[0]
                            session.receive_records(packet_io)
                        when :unbind
                            break
                        end
                    end
                end
                EM.cancel_timer(@timer) if @timer
                session.disconnected
            end

            def connection_completed()
                @fiber.resume(:connection_completed)
            rescue Exception => ex
                logger.error("Exception in connection_completed",ex)
            end

            def receive_records(packet_io)
                @fiber.resume(:receive_records,packet_io)
            rescue Exception => ex
                logger.error("Exception in receive_records",ex)
            end

            def disconnect()
                close_connection()
            end

            def unbind
                @fiber.resume(:unbind)
            rescue Exception => ex
                logger.error("Exception in unbind",ex)
            end

        end

        module Binding
            def connect(host,port,timeout)
                conn = EM.connect(host,port,ZooKeeper::EventMachine::ClientConn,self,timeout)
                conn.read_loop
            end
        end #class Binding

    end #module EventMachine
end #module ZooKeeper
