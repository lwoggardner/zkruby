require 'zkruby/socket'
module ZooKeeper

  class Connection
    include Slf4r::Logger
    include ZooKeeper::Protocol

    def initialize(host,port,timeout)
      super()
      @write_queue = Queue.new()
      @socket = Socket.tcp_connect_timeout(host,port,timeout)
      if socket
        socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
        socket.sync=true
      else
        logger.warn{"Connect timeout to #{host}:#{port}"} unless socket
      end
      write_loop
    rescue Errno::ECONNREFUSED
      logger.warn{"Connection refused to #{host}:#{port}"}
    end

    # This is called from random (but synchronized) client threads (including the event loop)
    def send_data(data)
      write_queue << data
    end

    def write_data(data)
      return unless socket && !socket.closed?
      socket.write(data) if data
      logger.debug { "Sent: #{data.unpack("H*")[0]}" }
    rescue StandardError => ex
      logger.warn( "Exception writing data to socket",ex )
      #It is ok to swallow this exception because we'll never get a reply
      disconnect()
    end

    def read_available(timeout)
      return nil unless socket && !socket.closed?
      #TODO: buffer size option?,read into a fixed buffer?
      socket.read_timeout(timeout,2048)
    end

    def close()
      disconnect
    end

    def write_loop
      #MRI is much faster if we write to a queue rather than directly to the socket,
      #not sure why, maybe thread contention of some kind?
      Thread.new() do
        while data = write_queue.pop
          write_data(data)
        end
        logger.debug { "Write queue complete" }
      end
    end
    private

    attr_reader :socket,:write_queue

    def disconnect()
      write_queue << nil
      socket.close if socket and !socket.closed?
    rescue Exception => ex
      #oh well
      logger.debug( "Exception closing socket", ex )
    end
  end
end

module ZooKeeperBinding

  # connect and read from the socket until disconnected
  def self.connect(host,port,timeout)
    ZooKeeper::Connection.new(host,port,timeout)
  end
end

