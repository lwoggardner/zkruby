require 'socket'
require 'thread'

# Binding over standard ruby sockets
#
# Manages 3 threads per zookeeper session
#
# Read thread
#   manages connecting to and reading from the tcp socket. Uses non blocking io to manage timeouts
#   and initiate the required ping requests.
#
# Write thread
#   each new connection spawns a new thread. Requests coming from the session in response
#   to multiple threads are written to a blocking queue. While the connection is alive
#   this thread reads from the queue and writes to the socket, all in blocking fashion
#   TODO: Is it really ok to do a non-blocking read during a blocking write?
# 
# Event thread
#   All response and watch callbacks are put on another blocking queue to be read and executed
#   by this thread.
#
# All interaction with the session is synchronized
#
# Client synchronous code is implemented with a condition variable that waits on the callback/errback 
#

# JRuby does not define Errno::NOERROR
unless defined? Errno::NOERROR 
    class Errno::NOERROR < SystemCallError
        Errno = 0
    end

end

module ZooKeeper::RubyIO

    class Connection
        include ZooKeeper::Protocol
        include Slf4r::Logger
        include Socket::Constants

        HAS_NONBLOCKING_CONNECT = RUBY_PLATFORM == "java" && Gem::Version.new(JRUBY_VERSION.dup) >= Gem::Version.new("1.6.7")
        SOL_TCP = IPPROTO_TCP unless defined? ::Socket::SOL_TCP

        attr_reader :session

        def initialize(host,port,timeout,session)
            @session = session
            @write_queue = Queue.new()

            if HAS_NONBLOCKING_CONNECT
                addr = Socket.getaddrinfo(host, nil)
                sock = Socket.new(Socket.const_get(addr[0][0]), Socket::SOCK_STREAM, 0)
                sock.setsockopt(SOL_SOCKET, SO_LINGER, [0,-1].pack("ii"))
                begin
                    sock.setsockopt(SOL_TCP, TCP_NODELAY,[0].pack("i_"))
                rescue
                    # JRuby defines SOL_TCP, but it doesn't work
                    sock.setsockopt(IPPROTO_TCP, TCP_NODELAY,[0].pack("i_"))
                end

                sockaddr = Socket.pack_sockaddr_in(port, addr[0][3])
                begin
                    sock.connect_nonblock(sockaddr)
                rescue Errno::EINPROGRESS
                  
                    begin
                        read,write,errors  = IO.select(nil, [sock], nil, timeout)
                    rescue Exception => ex
                        #JRuby raises Connection Refused instead of populating error array
                        logger.warn { "Exception #{ex.inspect} from non blocking select" }
                    end
                    optval = sock.getsockopt(Socket::SOL_SOCKET,Socket::SO_ERROR)
                    sockerr = (optval.unpack "i")[0]
                    if sockerr == Errno::NOERROR::Errno
                        #Woohoo! we're connected (lots of example code here will call
                        #connect_nonblock again to demonstrate EISCONN but I don't think
                        #this is strictly necessary
                    else
                        if sockerr == Errno::ECONNREFUSED::Errno
                            logger.warn("Connection refused to #{ host }:#{ port }")
                        else
                            logger.warn("Connection to #{ host }:#{ port } failed: #{sockerr}")
                        end
                        sock = nil
                    end
                end
            else
            # JRuby prior to 1.6.7 cannot do non-blocking connects, which means there is
            # no way to properly implement the connection-timeout
            # See http://jira.codehaus.org/browse/JRUBY-5165 
            # In any case this should be encapsulated in TCPSocket.open(host,port,timeout)
                logger.warn { "Using blocking connect (JRuby < 1.6.7)" }
                begin
                    sock = TCPSocket.new(host,port.to_i)
                rescue Errno::ECONNREFUSED
                    logger.warn("TCP Connection refused to #{host}:#{port}")
                    sock = nil
                end
            end

            Thread.new(sock) { |sock| write_loop(sock) } 
            read_loop(sock)
        end

        # This is called from random client threads
        def send_data(data)
            @write_queue.push(data)
        end

        # Since this runs in its very own thread and has no timers
        # we can use boring blocking IO
        def write_loop(socket)
            Thread.current[:name] = "ZooKeeper::RubyIO::WriteLoop"
            begin
                while socket
                    data = @write_queue.pop()
                    if socket.write(data) != data.length()
                        #TODO - will this really ever happen
                        logger.warn("Incomplete write!")
                    end
                    logger.debug { "Sending: " + data.unpack("H*")[0] }
                end
                logger.debug { "Write loop finished" }
            rescue Exception => ex
                logger.warn("Exception in write loop",ex)
                disconnect()
            end
        end

        def read_loop(socket)
            Thread.current[:name] = "ZooKeeper::RubyIO::ReadLoop"
            session.prime_connection(self) if socket
            ping = 0
            while socket # effectively forever since we never break it from this end
                begin
                    data = socket.read_nonblock(1024)
                    logger.debug { "Received (#{data.length})" + data.unpack("H*")[0] }
                    receive_data(data)
                    ping = 0
                rescue IO::WaitReadable
                    select_result = IO.select([socket],[],[],session.ping_interval)
                    unless select_result
                        ping += 1
                        # two timeouts in a row mean we need to send a ping
                        case ping
                        when 1 ;  session.ping()
                        when 2
                            logger.debug{"No response to ping in #{session.ping_interval}*2"}
                            break
                        end
                    end
                rescue EOFError
                    logger.debug { "EOF reading from socket" }
                    break
                rescue Exception => ex
                    logger.warn( "#{ex.class} exception in readloop",ex )
                    break
                end
            end
            disconnect(socket)
            session.disconnected()
        end

        # @api protocol
        def receive_records(packet_io)
            session.receive_records(packet_io)
        end

        private
        def disconnect(socket)
            socket.close if socket
        rescue Exception => ex
            #oh well
            logger.debug("Exception closing socket",ex)
        end

    end #Class connection

    module Binding
        def connect(host,port,timeout)
            Connection.new(host,port,timeout,self)
        end
    end
end

