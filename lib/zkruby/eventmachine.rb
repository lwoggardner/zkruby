require 'eventmachine'
require 'empathy'

if defined?(JRUBY_VERSION) && JRUBY_VERSION =~ /1\.6\.5.*/
  raise "Fibers are broken in JRuby 1.6.5 (See JRUBY-6170)"
end

module ZooKeeper
  module EventMachine

    class Connection < ::EM::Connection

      # Provides :send_data, :receive_data which match EM:Connection's requirements
      # Also :record_io
      include Protocol
      include Slf4r::Logger

      unless self.instance_methods.include?(:set_pending_connect_timeout)
        def set_pending_connect_timeout(timeout)
          logger.warn { "Unable to set pending connect timeout" }
        end
      end

      def initialize(timeout)
        super()
        @unbound = false
        set_pending_connect_timeout(timeout)
      rescue StandardError => ex
        logger.warn("Exception in initialize",ex)
      end

      def read_available(timeout)

        raise EOFError if unbound?

        @fiber = Fiber.current
        timer = ::EventMachine.add_timer(timeout)  { resume :read_timer }

        event,*args = Fiber.yield
        case event
        when :unbind
          ::EventMachine.cancel_timer(timer)
          raise EOFError
        when :receive_data
          ::EventMachine.cancel_timer(timer)
          args[0]
        when :read_timer
          nil
        else
          raise ProtocolError, "unexpected connection event"
        end
      ensure
        @fiber = nil
      end

      def send_data(data)
        super unless unbound?
      end

      def connection_completed()
        logger.debug { "Connection completed" }
      end

      def receive_data(data)
        resume(:receive_data,data)
      end

      def unbind(reason)
        @unbound = true
        logger.warn{"Connection #{self} unbound due to #{reason}"} if reason
        resume(:unbind, reason)
      end

      def close
        close_connection_after_writing
      end

      private
      def unbound?
        @unbound
      end

      def resume(event,*args)
        @fiber.resume(event,*args) if @fiber
      rescue StandardError => ex
        logger.error("Exception resuming #{@fiber} for event #{event}",ex)
      end
    end
  end #module EventMachine

end #module ZooKeeper

module Empathy

  module EM
    module ZooKeeperBinding
      def self.connect(host,port,timeout)
        ::EventMachine.connect(host,port,ZooKeeper::EventMachine::Connection,timeout)
      end
    end #class Binding
  end #module EM

  create_delegate_module('ZooKeeperBinding',:connect)

end #module Empathy

