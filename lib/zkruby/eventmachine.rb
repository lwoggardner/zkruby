require 'eventmachine'
require 'empathy'

if defined?(JRUBY_VERSION) && JRUBY_VERSION =~ /1\.6\.5.*/
    raise "Fibers are broken in JRuby 1.6.5 (See JRUBY-6170)"
end

module ZooKeeper
    module EventMachine

        class Connection < ::EM::Connection
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

            # This "loop" is a means of keeping all the session activity
            # on the session fiber
            def read_loop()
                event,*args = Fiber.yield
                if (event == :connection_completed)
                    logger.debug("Connection completed")
                    session.prime_connection(self)

                    @timer = ::EventMachine.add_timer(session.ping_interval) do
                        resume(:connect_timer)
                    end

                    ping = 0
                    unbound = false
                    # If session sleeps or waits in here then our yield/resumes are going to get out of sync
                    until unbound
                        event,*args = Fiber.yield
                        logger.debug { "Received event #{event} with #{args}" }
                        case event
                        when :connect_timer
                            if session.connected?
                                @timer = ::EventMachine.add_periodic_timer(session.ping_interval) do
                                    resume(:ping_timer) 
                                end
                            else
                                logger.warn("Connection timed out")
                                break;
                            end
                        when :ping_timer
                            case ping
                            when 1 then session.ping
                            when 2 then break;
                            end
                            ping += 1
                        when :receive_records
                            packet_io = args[0]
                            ping = 0
                            session.receive_records(packet_io)
                        when :unbind
                            unbound = true
                        else
                            logger.error("Unexpected resume - #{event}")
                            break;
                        end
                    end
                end
            ensure
                @fiber = nil
                ::EventMachine.cancel_timer(@timer) if @timer
                close_connection() unless unbound
            end

            def connection_completed()
                resume(:connection_completed)
            end

            def receive_records(packet_io)
                resume(:receive_records,packet_io)
            end

            def unbind(reason)
                logger.warn{"Connection #{self} unbound due to #{reason}"} if reason
                resume(:unbind)
            end

            private
            def resume(event,*args)
                @fiber.resume(event,*args) if @fiber
            rescue Exception => ex
                logger.error("Exception resuming #{@fiber} for event #{event}",ex)
            end
        end
    end #module EventMachine

end #module ZooKeeper

module Empathy

    module EM
        module ZooKeeperBinding
            def self.connect(session,host,port,timeout)
                conn = ::EventMachine.connect(host,port,ZooKeeper::EventMachine::Connection,session,timeout)
                conn.read_loop
            end
        end #class Binding
    end #module EM

    create_delegate_module('ZooKeeperBinding',:connect)

end #module Empathy

