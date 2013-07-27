require 'zkruby/socket'
module ZooKeeper

    class Connection
        include ZooKeeper::Protocol
        include Slf4r::Logger

        attr_reader :session

        def initialize(session)
            @session = session
        end

        def run(host,port,timeout)
            @write_queue = Queue.new()

            begin
                sock = Socket.tcp_connect_timeout(host,port,timeout)
                sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
                sock.sync=true
                write_thread = Thread.new(sock) { |sock| write_loop(sock) } 
                begin
                    session.prime_connection(self)
                    read_loop(sock)
                ensure
                    disconnect(sock)
                end
                write_thread.join
            rescue Errno::ECONNREFUSED
                logger.warn{"Connection refused to #{host}:#{port}"}
            end
        end

        # This is called from random client threads (including the event loop)
        def send_data(data)
            @write_queue.push(data) if data
        end

        private

        # Since this runs in its very own thread and has no timers
        # we can use boring blocking IO
        def write_loop(socket)
            Thread.current[:name] = "ZK::WriteLoop #{self} #{socket} #{session}"
            begin
                while !socket.closed? && data = @write_queue.pop()
                    socket.write(data) 
                    logger.debug { "Sent: #{data.unpack("H*")[0]}" }
                end
                logger.debug { "Write loop finished" }
            rescue Exception => ex
                logger.warn( "Exception in write loop",ex )
                # Make sure we break out of the read loop
                disconnect(socket)
            end
        end

        def read_loop(socket)
            ping = 0
            until socket.closed?
                begin
                    data = socket.read_timeout(512,session.ping_interval)
                    if data.nil?
                        logger.debug { "Read timed out" }
                        ping += 1
                        case ping
                        when 1 ; session.ping()
                        when 2 
                            logger.warn{"No response to ping in #{session.ping_interval}*2"}
                            break
                        end
                    else
                        logger.debug { "Received (#{data.length})" + data.unpack("H*")[0] }
                        receive_data(data)
                        ping = 0
                    end
                rescue EOFError
                    # This is how we expect to end - send a close packet and the
                    # server closes the socket
                    logger.debug { "EOF reading from socket" }
                    break
                end
            end
        end

        # @api protocol
        def receive_records(packet_io)
            session.receive_records(packet_io)
        end

        def disconnect(socket)
            @write_queue.push(nil)
            socket.close if socket and !socket.closed?
        rescue Exception => ex
            #oh well
            logger.debug("Exception closing socket",ex)
        end
    end
end

module ZooKeeperBinding

    # connect and read from the socket until disconnected
    def self.connect(session,host,port,timeout)
        ZooKeeper::Connection.new(session).run(host,port,timeout)
    end
end

