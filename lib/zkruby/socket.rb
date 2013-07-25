require  'socket'

class Socket

    HAS_NONBLOCKING_CONNECT =  RUBY_PLATFORM != "java"|| Gem::Version.new(JRUBY_VERSION.dup) >= Gem::Version.new("1.6.7")

    def self.tcp_connect_timeout(host,port,timeout = 0)

        if HAS_NONBLOCKING_CONNECT && timeout > 0
            # TODO: This is a blocking DNS lookup!!!!, possibly even a reverse lookup if host is a numberic address
            addr = self.getaddrinfo(host, nil)
            sock = Socket.new(self.const_get(addr[0][0]), Socket::SOCK_STREAM, 0)

            sockaddr = Socket.pack_sockaddr_in(port, addr[0][3])

            begin
                sock.connect_nonblock(sockaddr)
                return sock
            rescue Errno::EINPROGRESS
                begin
                    #Note: JRuby raises Connection Refused instead of populating error array
                    read,write,errors  = Socket.select(nil, [sock], [sock], timeout)
                    optval = sock.getsockopt(Socket::SOL_SOCKET,Socket::SO_ERROR)
                    sockerr = (optval.unpack "i")[0]
                end

                case sockerr
                when 0 # Errno::NOERROR::Errno
                    begin
                        sock.connect_nonblock(sockaddr)
                        return sock
                    rescue Errno::EISCONN
                        #Woohoo! we're connected
                        return sock
                    end
                when Errno::EINPROGRESS
                    # must be a timeout
                    logger.debug("Connect timeout")
                    return nil
                when Errno::ECONNREFUSED::Errno
                    raise Errno::ECONNREFUSED, "Connection refused to #{ host }:#{ port }"
                else
                    raise Errno::ENOTCONN, "Connection to #{ host }:#{ port } failed: #{sockerr}"
                end
            end
        else
            # JRuby prior to 1.6.7 cannot do non-blocking connects, which means there is
            # no way to properly implement the connection-timeout
            # See http://jira.codehaus.org/browse/JRUBY-5165 
            # In any case this should be encapsulated in TCPSocket.open(host,port,timeout)
            self.tcp(host,port) 
        end

    end

    def read_timeout(maxlen,timeout)
        begin
           return read_nonblock(maxlen)
        rescue IO::WaitReadable
            selected = IO.select([self],[],[],timeout)
            return nil unless selected
            retry
        end
    end
end


