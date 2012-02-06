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
        include Slf4r::Logger

        def receive_data data # :nodoc:

          @buffer ||= StringIO.new()
          @buffer.seek(0, IO::SEEK_END)
          @buffer << data
          @buffer.rewind
          logger.debug { "Received #{data.length} bytes: buffer length = #{@buffer.length} pos = #{@buffer.pos}" }
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
                    logger.debug { "Consumed packet #{packet_size}. Buffer pos=#{@buffer.pos}, length=#{@buffer.length}" }
                    next
                else
                    # found the last partial packet
                    @buffer.seek(-4, IO::SEEK_CUR)
                    logger.debug { "Buffer contains #{@buffer.length} of #{packet_size} packet" }
                end
            end
            break
          end
          # reset the buffer
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
            logger.debug { "Sent #{length} byte packet containing  #{records.length} records" }
        end
     end

    class Operation
        attr_reader :op, :opcode, :request, :response, :callback
        def initialize(op,opcode,request,response,callback)
            @op=op;@opcode=opcode
            @request=request;@response=response
            @callback=callback
        end
        
        def path
            #Every request has a path!
            #TODO - path may be chrooted!
            request.path if request.respond_to?(:path)
        end
    end

    class Packet < Operation
        attr_reader :xid, :watch_type, :watcher
       
        def initialize(xid,op,opcode,request,response,watch_type,watcher,callback)
            super(op,opcode,request,response,callback)
            @xid=xid;
            @watch_type = watch_type; @watcher = watcher
        end

        
        def error(reason)
            result(reason)[0..2] # don't need the watch
        end

        def result(rc)
            error = nil
            unless (Error::NONE === rc) then 
                error = Error.lookup(rc) 
                error = error.exception("ZooKeeper error for #{@op}(#{path}) ")
            end
            [ callback, error ,response, watch_type ] 
        end
    end

    # NoNode error is expected for exists
    class ExistsPacket < Packet
        def result(rc)
            Error::NO_NODE === rc ? [ callback, nil, nil, :exists ] : super(rc)
        end
    end

    # In the normal case the close packet will be last and will get
    # cleared via disconnected() and :session_expired
    class ClosePacket < Packet
        def result(rc)
            Error::SESSION_EXPIRED == rc ? [ callback, nil, nil, nil ] : super(rc)
        end
    end

    
    # Returned by asynchronous calls
    # 
    # @example
    #    op = zk.stat("\apath") { |stat| something_with_stat() }
    #
    #    op.on_error do |err|
    #      case err
    #      when ZK::Error::SESSION_EXPIRED 
    #           puts "Ignoring session expired"
    #      else
    #           raise err
    #      end
    #    end
    #
    #    begin
    #       result_of_somthing_with_stat = op.value
    #    rescue StandardError => ex
    #       puts "Oops, my error handler raised an exception"
    #    end
    #
    #
    class AsyncOp

        attr_accessor :backtrace

        # Provide an error callback. 
        # @param block the error callback as a block
        # @yieldparam [StandardError] the error raised by the zookeeper operation OR by its callback
        def errback(&block)
            set_error_handler(block)
        end

        # @param block the error callback as a Proc
        def errback=(block)
            set_error_handler(block)
        end

        alias :on_error :errback

        # Wait for the async op to finish and returns its value
        # will raise an exception if 
        #    the operation itself fails and there is no error handler
        #    the callback raises a StandardError and there is no error handler
        #    the error handler raises StandardError
        def value();
            wait_value()
            rescue ZooKeeper::Error => ex
               # Set the backtrace to the original caller, rather than the ZK event loop
               ex.set_backtrace(@backtrace) if @backtrace
               raise ex
        end

        private

        def set_error_handler(blk)
            raise NotImplementedError, ":wait_result to be privately implemented by binding"
        end

        def wait_value();
            raise NotImplementedError, ":wait_result to be privately implemented by binding"
        end
    end
end

