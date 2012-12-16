module ZooKeeper

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

        # @api private
        def initialize(event_loop,callback,&operation)
            @event_loop = event_loop
            @operation = operation
            @callback = callback
            @mutex,@cv = Strand::Mutex.new(), Strand::ConditionVariable.new()
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
        # @param matches [Class,...] subclasses of Exception to match, defaults to {::StandardError}
        # @param errblock the block to call if an error matches
        # @return self
        # @yieldparam [Exception] ex the exception raised by the async operation OR by its callback
        def async_rescue(*matches,&errblock)
            matches << StandardError if matches.empty?
            rescue_blocks << [ matches ,errblock ]
            if @op_error && matches.any? { |m| m  === @op_error }
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
            self
        end
        alias :on_error :async_rescue
        alias :op_rescue :async_rescue

        # @deprecated
        alias :errback :async_rescue


        # Must only be called inside a block supplied to {#async_rescue}
        # Common case is to retry on connection lost
        # Retrying :session_expired is guaranteed to give infinite loops!
        def async_retry()
            raise ProtocolError "cannot retry outside of a rescue block" unless @allow_retry
            begin
                execute()
                nil
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
        
        # @api private
        attr_accessor :backtrace

        # @api private
        def resume(op_error,response)
            mutex.synchronize do
                # remember this mutex is only used to wait for this response anyway
                # so synchronizing here is not harmful even if processing the response
                # includes a long running callback. (which can't create deadlocks
                # by referencing this op!
                @resumed = true
                @error, @result = process_response(op_error,response)
                cv.signal() if resumed? 
            end
        end

        private
        attr_reader :callback, :operation, :event_loop
        attr_reader :mutex, :cv, :error, :result
        
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

        def rescue_blocks
            @rescue_blocks ||= []
        end

        def process_response(op_error,response)
            logger.debug { "Processing response #{op_error} #{response}" }
            
            begin
                return [ nil, callback.call(response) ] unless op_error
            rescue Exception => ex #enable clients to rescue Exceptions
                op_error = ex
            end

            matches,rb = rescue_blocks.detect() { |matches,errblock| matches.any? { |m| m === op_error } }
            return [ op_error, nil ] unless rb

            begin
                @allow_retry = true
                return [ nil, rb.call(op_error) ]
            rescue StandardError => ex
                return [ ex , nil ]
            ensure
                @allow_retry = false
            end
        end

        def wait_value()
            if event_loop.current?
                #Waiting in the event loop (eg made a synchronous call inside a callback)
                #Keep processing events until we are resumed
                until resumed? || event_loop.dead?
                    event_loop.pop_event_queue()
                end
            else
                mutex.synchronize { 
                    cv.wait(mutex) unless resumed?
                }
            end

            raise error if error
            result
        end
    end
end
