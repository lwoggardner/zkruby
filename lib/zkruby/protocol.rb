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
    #    op.async_rescue ZK::Error::SESSION_EXPIRED do 
    #        puts "Ignoring session expired"
    #        result_when_session_expired()
    #    end
    #    
    #    op.async_rescue ZK::Error::CONNECTION_LOST do |ex|
    #        puts "Retrying stat due to connection lost"
    #        op.async_retry()
    #        # the result of this block is ignored!
    #    end
    #
    #    begin
    #       result_of_somthing_with_stat = op.value
    #    rescue StandardError => ex
    #       puts "Oops"
    #    end
    #
    #
    class AsyncOp

        # @api binding
        # @note Binding API, not for client use
        attr_accessor :backtrace

        # @api binding
        # @note Binding API, not for client use
        def initialize(callback,&operation)
            self.operation = operation
            self.callback = callback
            begin
                execute()
            rescue ZooKeeper::Error => ex
                # Capture any initial exception
                # The only expected condition is :session_expired
                @op_error = ex
                logger.debug { "Error during initial call #{ex}" }
            end
        end

        # @param [Proc,#to_proc] block the error callback as a Proc
        # @deprecated use {#async_rescue}
        def errback=(block)
            async_rescue(&block)
        end

        #Rescue asynchronous exceptions in a similar manner to normal
        #ruby. Unfortunately rescue is a reserved word
        # @param matches [Class,...] subclasses of Exception to match, defaults to {StandardError}
        # @param errblock the block to call if an error matches
        # @yieldparam [Exception] ex the exception raised by the async operation OR by its callback
        def async_rescue(*matches,&errblock)
            matches << StandardError if matches.empty?
            matches.each do |match|
                rescue_blocks << [ match ,errblock ]
                if @op_error && match === @op_error
                    begin
                        @allow_retry = true
                        @op_rescue= [ nil, errblock.call(@op_error) ]
                    rescue StandardError => ex
                        @operation_rescue_result = [ ex, nil ]
                    ensure
                        @resumed = true
                        @op_error = nil
                        @allow_retry = false
                    end
                end
            end
        end
        alias :on_error :async_rescue
        alias :op_rescue :async_rescue

        # @deprecated
        alias :errback :async_rescue


        # Must only be called inside a block supplied to {#async_rescue}
        # Common case is to retry on connection lost
        # Retrying :session_expired is guaranteed to give infinite loops!
        def async_retry()
            raise ProtocolError "trying to retry outside of a rescue block" unless @allow_retry
            begin
                execute()
            rescue ZooKeeper::Error => ex
                error,result = process_response(ex,nil)
                if resumed?
                    raise error if error
                    return result
                end
            end
        end

        alias :op_retry :async_retry
        alias :try_again :async_retry

        # Wait for the async op to finish and returns its value
        # @return result of the operation's callback or matched rescue handler
        # @raise [StandardError] any unrescued exception
        def value();
            if @op_error
                raise @op_error
            elsif @op_rescue
                error, result = @op_rescue
                raise error if error
                return result
            else
                begin
                    wait_value()
                rescue ZooKeeper::Error => ex
                    # Set the backtrace to the original caller, rather than the ZK event loop
                    ex.set_backtrace(@backtrace) if @backtrace
                    raise ex
                end
            end
        end

        # @api binding
        # @note Binding API, not for client use
        def resume(error,response)
            process_resume(error,response)
        end

        protected
        attr_accessor :callback, :operation

        private
        def execute()
            @op_error = nil
            @op_rescue = nil
            @resumed = false
            operation.call(self)
            true
        end

        def resumed?
            @resumed 
        end

        def process_resume(error,response)
            raise NotImplementedError, ":process_resume to be privately implemented by binding"
        end

        def wait_value();
            raise NotImplementedError, ":wait_result to be privately implemented by binding"
        end

        def rescue_blocks
            @rescue_blocks ||= []
        end

        def process_response(error,response)
            @resumed = true
            begin
                return [ nil, callback.call(response) ] unless error
            rescue Exception => ex #enable clients to rescue Exceptions
                error = ex
            end

            match,rb = rescue_blocks.detect() { |match,errblock| match === error }
            return [ error, nil ] unless rb

            begin
                @allow_retry = true
                return [ nil, rb.call(error) ]
            rescue StandardError => ex
                return [ ex , nil ]
            ensure
                @allow_retry = false
            end

        end
    end
end

