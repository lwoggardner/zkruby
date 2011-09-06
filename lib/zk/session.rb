require 'set'
module ZooKeeper
    
     # Represents an session that may span connections
     class Session

        DEFAULT_TIMEOUT = 4
        DEFAULT_CONNECT_DELAY = 0.2
        include Slf4r::Logger

        attr_reader :ping_interval
        attr_reader :timeout
        attr_reader :conn
        attr_accessor :watcher

        def initialize(binding,addresses,options=nil)
            
            @binding = binding

            @addresses = parse_addresses(addresses)
            parse_options(options)
            
            # These are the server states
            # :disconnected, :connected, :auth_failed, :expired
            @keeper_state = nil

            # Client state is
            # :ready, :closing, :closed
            @client_state = :ready

            @xid=0
            @pending_queue = []

            # Create the watch list
            # hash by watch type of hashes by path of set of watchers
            @watches = [ :children, :data, :exists ].inject({}) do |ws,wtype| 
                ws[wtype] = Hash.new() { |h,k| h[k] = Set.new() }
                ws
            end

            @watcher = nil

        end
    
        def chroot(path)
            return @chroot + path
        end

        def unchroot(path)
            return path unless path
            path.slice(@chroot.length..-1)
        end


        # Connection API - testing whether to send a ping
        def connected?()
            @keeper_state == :connected
        end

        # Connection API - Injects a new connection that is ready to receive records
        # @param conn that responds to #send_records(record...) and #disconnect()
        def prime_connection(conn)
            @conn = conn
            send_session_connect()
            send_auth_data()
            reset_watches()
        end
        

        # Connection API - called when data is available, reads and processes one packet/event
        # @param io <IO> 
        def receive_records(io)
           case @keeper_state
           when :disconnected
              complete_connection(io)
           when :connected
              process_reply(io)
           else
              logger.warn { "Receive packet for closed session #{@keeper_state}" }
           end
        end

        # Connection API - called when no data has been received for #ping_interval
        def ping()
            if @keeper_state == :connected
                logger.debug { "Ping send" }
                hdr = Proto::RequestHeader.new(:xid => -2, :_type => 11)
                conn.send_records(hdr)
            end
        end

        # Connection API - called when the connection has dropped from either end
        def disconnected()
           @conn = nil
           logger.info { "Disconnected id=#{@session_id}, keeper=:#{@keeper_state}, client=:#{@client_state}" }
          
           # We keep trying to reconnect until the session expiration time is reached
           @disconnect_time = Time.now if @keeper_state == :connected
           time_since_first_disconnect = (Time.now - @disconnect_time) 

           if @client_state == :closed || time_since_first_disconnect > timeout
                session_expired()
           else
                # if we are connected then everything in the pending queue has been sent so
                # we must clear
                # if not, then we'll keep them and hope the next reconnect works
                if @keeper_state == :connected
                    clear_pending_queue(:disconnected)
                    invoke_watch(@watcher,KeeperState::DISCONNECTED,nil,WatchEvent::NONE) if @watcher
                end
                @keeper_state = :disconnected
                reconnect()
           end
        end

        # Start the session - called by the ProtocolBinding
        def start()
            raise ProtocolError, "Already started!" unless @keeper_state.nil?
            @keeper_state = :disconnected
            @disconnect_time = Time.now
            logger.debug ("Starting new zookeeper client session")
            reconnect()
        end
       
        def queue_request(request,op,opcode,response=nil,watch_type=nil,watcher=nil,ptype=Packet,&callback)
            raise ZooKeeperError.new(:session_expired) unless @client_state == :ready

            watch_type, watcher = resolve_watcher(watch_type,watcher)

            xid = next_xid

            packet = ptype.new(xid,op,opcode,request,response,watch_type,watcher, callback)
            
            queue_packet(packet)
            
            QueuedOp.new(packet)
        end

        def close(&blk)
            #TODO possibly this should not be an exception
            #TODO although if not an exception, perhaps should yield the block
            raise ZooKeeperError.new(:session_expired) unless @client_state == :ready

            # we keep the requested block in a close packet
            @close_packet = ClosePacket.new(next_xid(),:close,-11,nil,nil,nil,nil,blk)
            close_packet = @close_packet 
            @client_state = :closing

            # If there are other requests in flight, then we wait for them to finish
            # before sending the close packet since it immediately causes the socket
            # to close.
            queue_close_packet_if_necessary()
            QueuedOp.new(close_packet)
        end
        private
        attr_reader :watches
        attr_reader :binding

        def parse_addresses(addresses)
            case addresses
            when String
                parse_addresses(addresses.split(","))
            when Array
                result = addresses.collect() { |addr| parse_address(addr) }
                #Randomise the connection order
                result.shuffle!
            else
                raise ArgumentError "Not able to parse addresses from #{addresses}"
            end
        end

        def parse_address(address)
            case address
            when String
                host,port = address.split(":")
                port = DEFAULT_PORT unless port
                [host,port]
            when Array
                address[0..1]
            end
        end
        
        def parse_options(options)
            @timeout = options.fetch(:timeout,DEFAULT_TIMEOUT)
            @max_connect_delay = options.fetch(:connect_delay,DEFAULT_CONNECT_DELAY)
            @connect_timeout = options.fetch(:connect_timeout,@timeout * 1.0 / 7.0)
            @scheme = options.fetch(:scheme,nil)
            @auth = options.fetch(:auth,nil)
            @chroot = options.fetch(:chroot,"").chomp("/")
        end

        def reconnect()
           
            #Rotate address
            host,port = @addresses.shift
            @addresses.push([host,port])

            delay = rand() * @max_connect_delay
            
            logger.debug { "Connecting id=#{@session_id} to #{host}:#{port} with delay=#{delay}, timeout=#{@connect_timeout}" } 
            binding.connect(host,port,delay,@connect_timeout)
        end

       
        def session_expired(reason=:expired)
           clear_pending_queue(reason)
           
           invoke_response(*@close_packet.error(reason)) if @close_packet

           if @client_state == :closed
              logger.info { "Session closed id=#{@session_id}, keeper=:#{@keeper_state}, client=:#{@client_state}" }
           else
              logger.warn { "Session expired id=#{@session_id}, keeper=:#{@keeper_state}, client=:#{@client_state}" }
           end

           invoke_watch(@watcher,KeeperState::EXPIRED,nil,WatchEvent::NONE) if @watcher
           @keeper_state = reason
           @client_state = :closed
        end

        def complete_connection(response)
            result = Proto::ConnectResponse.read(response)
            if (result.time_out <= 0)
                #We're dead!
                session_expired()
            else
                @timeout = result.time_out.to_f / 1000.0
                @keeper_state = :connected

                # Why 2 / 7 of the timeout?. If a binding sees no server response in this period it is required to
                # generate a ping request
                # if 2 periods go by without activity it is required to disconnect
                # so we are alrea
                @ping_interval = @timeout * 2.0 / 7.0
                @session_id = result.session_id
                @session_passwd = result.passwd
                logger.info { "Connected session_id=#{@session_id}, timeout=#{@time_out}, ping=#{@ping_interval}" }

                logger.debug { "Sending #{@pending_queue.length} queued packets" }
                @pending_queue.each { |p| send_packet(p) }
                
                queue_close_packet_if_necessary()
                invoke_watch(@watcher,KeeperState::CONNECTED,nil,WatchEvent::NONE) if @watcher
            end
        end

        def send_session_connect() 
            req = Proto::ConnectRequest.new( :timeout => timeout )
            req.last_zxid_seen = @last_zxid_seen if @last_zxid_seen
            req.session_id =  @session_id if @session_id
            req.passwd = @session_passwd if @session_passwd

            conn.send_records(req)
        end

        def send_auth_data()
            if @scheme
                req = Proto::AuthPacket.new(:scheme => @scheme, :auth => @auth)
                packet = Packet.new(-4,:auth,100,req,nil,nil,nil,nil)
                send_packet(packet)
            end
        end
        # Watches are dropped on disconnect, we reset them here
        # dropping connections can be a good way of cleaning up on the server side
        # #TODO If watch reset is disabled the watches will be notified of connection loss
        # otherwise they will be seemlessly re-added
        # This way a watch is only ever triggered exactly once
        def reset_watches()
            unless watches[:children].empty? && watches[:data].empty? && watches[:exists].empty?
               req = Proto::SetWatches.new()
               req.relative_zxid = @last_zxid_seen
               req.data_watches = watches[:data].keys
               req.child_watches = watches[:children].keys
               req.exist_watches = watches[:exists].keys

               packet = Packet.new(-8,:set_watches,101,req,nil,nil,nil,nil)
               send_packet(packet)
            end
        end
        
        def process_reply(packet_io)
              header = Proto::ReplyHeader.read(packet_io)
              logger.debug { "Reply header: #{header.inspect}" }

              case header.xid.to_i
              when -2
                logger.debug { "Ping reply" }
              when -4
                logger.debug { "Auth reply" }
                session_expired(:auth_failed) unless header.err.to_i == 0
              when -1
                #Watch notification
                event = Proto::WatcherEvent.read(packet_io)
                logger.debug { "Watch notification #{event.inspect} " }
                process_watch_notification(event.state.to_i,event.path,event._type.to_i)
              when -8
                #Reset watch reply
                logger.debug { "SetWatch reply"}
                #TODO If error, send :disconnected to all watches
              else
                # A normal packet reply. They should come in the order we sent them
                # so we just match it to the packet at the front of the queue
                packet = @pending_queue.shift
                logger.debug { "Packet reply: #{packet.inspect}" }

                if (packet.xid.to_i != header.xid.to_i)

                   logger.error { "Bad XID! expected=#{packet.xid}, received=#{header.xid}" }

                   # Treat this like a dropped connection, and then force the connection
                   # to be dropped. But wait for the connection to notify us before
                   # we actually update our keeper_state
                   packet.error(:disconnected)
                   @conn.disconnect() 
                else
                    
                    @last_zxid_seen = header.zxid
                    callback, response, watch_type  = packet.result(header.err.to_i)
                    logger.debug { "Reply response: #{response.inspect}" } 
                    invoke_response(callback,response,packet_io)

                    if (packet.watch_type) 
                        @watches[packet.watch_type][packet.path] << packet.watcher 
                        logger.debug { "Registered #{packet.watcher} for type=#{packet.watch_type} at #{packet.path}" }
                    end
                    queue_close_packet_if_necessary()
                end
              end
        end
        

        def process_watch_notification(state,path,event)
            
            watch_event = WatchEvent.fetch(event)
            watch_types = watch_event.watch_types()

            watches = watch_types.inject(Set.new()) do | result, watch_type |
               more_watches = @watches[watch_type].delete(path)
               result.merge(more_watches) if more_watches
               result
            end
            
            if watches.empty?
                logger.warn ( "Received notification for unregistered watch #{state} #{path} #{event}" )
            end
            watches.each { | watch | invoke_watch(watch,state,path,watch_event) }      
             
        end

        def invoke_watch(watch,state,path,event)
                logger.debug { "Watch #{watch} triggered with #{state}, #{path}. #{event}" }
                if watch.respond_to?(:process_watch)
                   callback = Proc.new() { |state,path,event| watch.process_watch(state,path,event) } 
                elsif watch.respond_to?(:call)
                   callback = watch
                else
                   raise ProtocolError("Bad watcher #{watch}")
                end

                binding.invoke(callback,state,unchroot(path),event)
        end

        def clear_pending_queue(reason)
           @pending_queue.each  { |p| invoke_response(*p.error(reason)) }
           @pending_queue.clear
        end

        def queue_close_packet_if_necessary
            if @pending_queue.empty? && @keeper_state == :connected && @close_packet
                logger.debug { "Sending close packet!" }
                @client_state = :closed
                queue_packet(@close_packet)
                @close_packet = nil
            end
        end

        def invoke_response(callback,response,packet_io = nil)
            if callback
                args = if response.respond_to?(:read) && packet_io
                    [response.read(packet_io)]
                elsif response
                    [response]
                else
                    []
                end
                binding.invoke(callback,*args)
            end
        end

        def resolve_watcher(watch_type,watcher)
            if watcher == true && @watcher
                #the actual TrueClass refers to the default watcher
                watcher = @watcher
            elsif watcher.respond_to?(:call) || watcher.respond_to?(:process_watch)
                # ok a proc or quacks like a watcher
            elsif watcher
                # something, but not something we can handle
                raise ArgumentError, "Not a watcher #{watcher.inspect}"
            else
                watch_type = nil
            end
            [watch_type,watcher]
        end


        def queue_packet(packet)
            @pending_queue.push(packet)
            logger.debug { "Queued: #{packet.inspect}" }

            if @keeper_state == :connected
                send_packet(packet)
            end
        end

        def next_xid
            @xid += 1
        end

        def send_packet(packet)
            records = [] << Proto::RequestHeader.new(:xid => packet.xid, :_type => packet.opcode)
            records << packet.request if packet.request
            conn.send_records(*records)
        end


    end # Session
end
