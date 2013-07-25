require 'socket'

class Socket
    HAS_NONBLOCKING_CONNECT =  RUBY_PLATFORM != "java"|| Gem::Version.new(JRUBY_VERSION.dup) >= Gem::Version.new("1.6.7")
    
    # We add these commonly used patterns to socket
    def connect_timeout(host,port,timeout)
        # NOTE for Sockets - connect timeout can still block resolving host to an ip address

    end

    def read_timeout(maxlen,buffer,timeout)

    end
end

module Strand
       include Socket::Constants 
        SOL_TCP = ::Socket::IPPROTO_TCP unless defined? ::Socket::SOL_TCP

    def self.tcpsocket(host,port,options)
        local_host = options[:local_host] || 0
        local_port = options[:local_port] || 9
        timeout    = options[:timeout] 

        #TODO Validate args

        if Strand.event_machine?
            # TODO: EM has no way of requesting local host, local port
            # But presumably could do so
            # Maybe can set via socket options? or connection method to be called during initialize
            # to set localhost, port
            sock = EM::Socket.connect(host,port,timeout)
        if HAS_NONBLOCKING_CONNECT && timeout
                #This whole attempt and non blocking is screwed because the DNS lookup is blocking
                #and puts a serious dent in the aim for ZKRUBY to be non-blocking
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
                        optval = sock.getsockopt(Socket::SOL_SOCKET,Socket::SO_ERROR)
                        sockerr = (optval.unpack "i")[0]
                    rescue Exception => ex
                        #JRuby raises Connection Refused instead of populating error array
                        #Strand has nologging
                        #logger.warn( "Exception from non blocking select", ex )
                        raise
                    end

                    if sockerr == Errno::NOERROR::Errno
                        begin
                            sock.connect_nonblock(sockaddr)
                        rescue Errno::EISCONN
                            #Woohoo! we're connected
                        rescue Exception => ex
                            #logger.warn( "Exception after successful connect",ex )
                            raise
                        end
                    else
                        if sockerr == Errno::ECONNREFUSED::Errno
                            raise Errno::ECONNREFUSED, "Connection refused to #{ host }:#{ port }"
                        else
                            raise Errno::ECONNREFUSED, "Connection to #{host}:#{port} failed with sockerr=#{sockerr}"
                        end
                    end
                end
            else
            # JRuby prior to 1.6.7 cannot do non-blocking connects, which means there is
            # no way to properly implement the connection-timeout
            # See http://jira.codehaus.org/browse/JRUBY-5165 
            # In any case this should be encapsulated in TCPSocket.open(host,port,timeout)
                #logger.warn { "Using blocking connect (JRuby < 1.6.7)" }
                begin
                    #TODO: what to do about timeout?
                    sock = TCPSocket.new(host,port,local_host,local_port)
                rescue Errno::ECONNREFUSED
                    raise
                end
            end
            return sock
    end
end

module Strand::EM
    #Quacks like a socket, but is really EventMachine connection
    class Socket < ::EM::Connection

        attr_accessor :sync

        def self.connect(host,port,timeout)
            sock = ::EM.connect(host,port,self,timout)
            sock.connected?() || raise 

        def initialize(timeout=0)
            set_pending_connect_timeout(timeout) unless timeout <= 0
        end

        def connection_completed
           notify(:connection_completed)
        end

        def unbind(error=0)
           # a strand waiting for a connection should get a raise (exception) here
           # a connection waiting on close shoudl get a wakeup (here)
           unbound?
           notify(:unbind,error)
        end

        def eof?
            wait_for(:receive_data,:unbind)
            return unbound?
        end

        # This is a blocking method
        def close()
            flush()
            close_connection_after_writing()
            wait_for(:unbind)
        end

        def gets()
            #ugly - block until separatr
        end

        def read(length=nil,buffer=nil)

        end

        def readpartial()

        end
        def <<(data)
            send_data(data)
        end

        def flush()
         # May not be necessary since we always immediately call send_data 
         # and we have no way of knowing whether it has been sent
        end

        def closed?()
            return unbound?
        end
    end
end
