require 'stringio'

module ZooKeeper
    # Represents a problem communicating with the ZK cluster
    # client's shouldn't see this
    class ProtocolError < IOError;end

     # Raw object protocol, very similar to EM's native ObjectProtocol
     # Expects:
     #   #receive_data and #send_records to be invoked
     #   #receive_records and #send_data to be implemented
     module Protocol
        MIN_PACKET = 5 #TODO Work out what the min packet size really is

        def receive_data data # :nodoc:
          (@buffer ||= StringIO.new()) << data
          @buffer.rewind
          loop do
            if @buffer.length - @buffer.pos > MIN_PACKET 
                packet_size = @buffer.read(4).unpack("N").first
                if (@buffer.length - @buffer.pos >= packet_size)
                    expected_pos = @buffer.pos + packet_size
                    # We just pass the buffer around and expect packet_size to be consumed
                    receive_records(@buffer)
                    if (@buffer.pos != expected_pos)
                        #this can happen during disconnection with left over packets
                        #the connection is dying anyway
                        leftover = @buffer.read(packet_size).unpack("H*")[0]
                        raise ProtocolError, "Records not consumed #{leftover}"
                    end
                    
                    next
                else
                    # found the last partial packet
                    @buffer.seek(-4, IO::SEEK_CUR)
                end
            end
            break
          end 
          @buffer = StringIO.new(@buffer.read()) if @buffer.pos > 0
        end

        def receive_records(packet_io)
            #stub
            #we don't unpack records here because we don't know what kind of records they are!
        end

        def send_records(*records)
            length = 0
            bindata = records.collect { |r| s = r.to_binary_s; length += s.length; s }
            send_data([length].pack("N"))
            bindata.each { |b| send_data(b) }
        end
     end

    class Packet
        attr_reader :xid,:op,:opcode,:request,:response, :watch_type, :watcher, :callback
        attr_accessor :errback
       
        def initialize(xid,op,opcode,request,response,watch_type,watcher,callback)
            @xid=xid;@op=op;@opcode=opcode
            @request=request;@response=response
            @watch_type = watch_type; @watcher = watcher
            @callback=callback
        end

        def path
            #Every request has a path!
            request.path
        end

        def error(reason)
            result(reason)[0..1] # don't need the watch
        end

        def result(rc)
            Error::NONE === rc ? [ callback, response, watch_type ] : [ errback, rc, nil ] 
        end
    end

    # NoNode error is expected for exists
    class ExistsPacket < Packet
        def result(rc)
            Error::NO_NODE === rc ? [ callback, nil, :exists ] : super(rc)
        end
    end

    # In the normal case the close packet will be last and will get
    # cleared via disconnected() and :session_expired
    class ClosePacket < Packet
        def result(rc)
            Error::SESSION_EXPIRED == rc ? [ callback, nil, nil ] : super(rc)
        end
    end
end

