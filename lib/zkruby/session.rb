require 'set'
require 'thread'
require 'monitor'
require 'zkruby/async_op'
module ZooKeeper

    # Represents a session that may span connect/disconnect
    #
    # @note this is a private API not intended for client use
    class Session
        # There are multiple threads of execution involved in a session
        # Client threads - send requests
        # Connection/Read thread
        # EventLoop - callback execution 
        # 
        # All client activity is synchronised on this session as a monitor
        # The read thread synchronises with client activity 
        # only around connection/disconnection
        # ie any changes to @session_state which always happens in
        # conjunction with processing all entries in @pending_queue
        #
        # All interaction with the event loop occurs via a Queue
        include MonitorMixin

        DEFAULT_TIMEOUT = 4
        DEFAULT_CONNECT_DELAY = 0.2
        DEFAULT_PORT = 2181

        include Slf4r::Logger

        attr_reader :ping_interval
        attr_reader :ping_logger
        attr_reader :timeout
        attr_reader :conn
        attr_accessor :watcher

        # @api zookeeper
        # See {ZooKeeper.connect}
        def initialize(addresses,options=nil)
            super()
            @addresses = parse_addresses(addresses)
            parse_options(options)

            # our transaction id
            @xid=0

            # The state of the connection, nil, :disconnected, :connected, :closed, :expired
            @session_state = nil

            # The connection we'll send packets to
            @conn = nil

            # The list of pending requests
            # When disconnected this builds up a list to send through when we are connected
            # When connected represents the order in which we expect to see responses
            @pending_queue = []

            # Client state is :ready, :closing, :closed
            @client_state = :ready

            # Create the watch list
            # hash by watch type of hashes by path of set of watchers
            @watches = [ :children, :data, :exists ].inject({}) do |ws,wtype| 
                ws[wtype] = Hash.new() { |h,k| h[k] = Set.new() }
                ws
            end

            # the default watcher
            @watcher = nil

            @ping_logger = Slf4r::LoggerFacade.new("ZooKeeper::Session::Ping")
        end

        # @api connection
        # Injects a new connection that is ready to receive records
        # @param conn that responds to #send_records(record...) and #disconnect()
        def prime_connection(conn)
            @conn = conn
            send_session_connect()
            send_auth_data()
            reset_watches()
        end

        # @api connection
        # called when data is available, reads and processes one packet/event
        # @param [IO] io
        def receive_records(io)
            case @session_state
            when :disconnected
                complete_connection(io)
            when :connected
                process_reply(io)
            else
                logger.warn { "Receive packet for closed session #{@session_state}" }
            end
        end

        # @api connection
        # Connection API - testing whether to send a ping
        def connected?()
            @session_state == :connected
        end


        # @api connection
        # called when no data has been received for #ping_interval
        def ping()
            if connected?
                ping_logger.debug { "Ping send" }
                hdr = Proto::RequestHeader.new(:xid => -2, :_type => 11)
                conn.send_records(hdr) 
            end
        end

        # TODO: Merge all this into a connect loop called from start
        def disconnected()
            logger.info { "Disconnected id=#{@session_id}, keeper=:#{@session_state}, client=:#{@client_state}" }

            # We keep trying to reconnect until the session expiration time is reached
            @disconnect_time = Time.now if @session_state == :connected
            time_since_first_disconnect = (Time.now - @disconnect_time) 

            if @session_state == :closing
                #We were expecting this disconnect
                session_expired(:closed)
            elsif time_since_first_disconnect > timeout
                session_expired(:expired)
            else
                if @session_state == :connected
                    #first disconnect
                    clear_pending_queue(:disconnected)
                    invoke_watch(@watcher,KeeperState::DISCONNECTED,nil,WatchEvent::NONE) if @watcher
                    @conn = nil
                end
            end
        end

        # @api zookeeper
        # See {ZooKeeper.connect}
        def start(client)
            raise ProtocolError, "Already started!" unless @session_state.nil?
            @session_state = :disconnected
            @disconnect_time = Time.now
            logger.debug {"Starting new zookeeper client session for #{client}"}
            @event_loop = EventLoop.new(client)
            # This is the read/connect thread
            Thread.new {
                Thread.current[:name] = "ZK::Session #{self}"
                reconnect()
                while active?
                    delay = rand() * @max_connect_delay
                    sleep(delay)
                    reconnect()
                end
                logger.debug {"Session #{self} complete" }
            }
        end


        # @api client
        def chroot(path)
            return @chroot if path == "/"
            return @chroot + path
        end

        # @api client
        def unchroot(path)
            return path unless path
            path.slice(@chroot.length..-1)
        end

        # @api client
        def request(*args,&callback)
            AsyncOp.new(@event_loop,callback) do |op|
                queue_request(*args) do |error,response|
                    op.resume(error,response)
                end
            end
        end

        # @api client
        def close(&callback)
            AsyncOp.new(@event_loop,callback) do |op|
                close_session() do |error,response|
                    op.resume(error,response) 
                end
            end
        end

        private
        def active?
            [:connected,:disconnected].include?(@session_state)
        end

        def calculate_timeouts()
            @ping_interval = timeout * 2.0/7.0
            @connect_timeout = timeout / 2.0
        end

        def queue_request(request,op,opcode,response=nil,watch_type=nil,watcher=nil,ptype=Packet,&callback)
            synchronize do 
                raise ProtocolError, "Client closed #{@client_state}" unless @client_state == :ready
                raise Error.SESSION_EXPIRED, "Session has expired #{@session_state}" unless active?
                watch_type, watcher = resolve_watcher(watch_type,watcher)

                xid = next_xid

                packet = ptype.new(xid,op,opcode,request,response,watch_type,watcher, callback)

                queue_packet(packet)
            end
        end

        def close_session(&callback)
            synchronize do
                if @client_state == :ready
                    if active?
                        # we keep the requested block in a close packet but we don't send it
                        # until we've received all pending reponses
                        @close_packet = ClosePacket.new(next_xid(),:close,-11,nil,nil,nil,nil,callback)

                        # but we can force a response by sending a ping
                        ping()
                        
                    else
                        # We've already expired put the close callback on the event loop
                        @event_loop.invoke_close(callback,nil,true) 
                    end
                    @client_state = :closed
                else
                    raise ProtocolError, "Client already #{@client_state}"
                end
            end
        end

        attr_reader :watches

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
            calculate_timeouts()
            @max_connect_delay = options.fetch(:connect_delay,DEFAULT_CONNECT_DELAY)
            @scheme = options.fetch(:scheme,nil)
            @auth = options.fetch(:auth,nil)
            @chroot = options.fetch(:chroot,"").chomp("/")
        end

        def reconnect()
            #Rotate address
            host,port = @addresses.shift
            @addresses.push([host,port])
            logger.debug { "Connecting id=#{@session_id} to #{host}:#{port} with timeout=#{@connect_timeout} #{ZooKeeperBinding.inspect}" } 
            begin
                ZooKeeperBinding.connect(self,host,port,@connect_timeout)
            rescue Exception => ex
                logger.warn("Exception in connect loop", ex)
            ensure
                disconnected()
            end
        end

        def session_expired(reason=:expired)
            if reason == :closed
                logger.info { "Session closed id=#{@session_id}, keeper=:#{@session_state}, client=:#{@client_state}" }
            else
                logger.warn { "Session expired reason=#{reason} id=#{@session_id}, keeper=:#{@session_state}, client=:#{@client_state}" }
            end

            clear_pending_queue(reason)
            #TODO Clients will want to distinguish between EXPIRED and CLOSED
            invoke_watch(@watcher,KeeperState::EXPIRED,nil,WatchEvent::NONE) if @watcher
            @event_loop.stop()
        end

        def complete_connection(response)
            result = Proto::ConnectResponse.read(response)
            if (result.time_out <= 0)
                #We're dead!
                session_expired() 
            else
                @timeout = result.time_out.to_f / 1000.0
                calculate_timeouts()
                @session_id = result.session_id
                @session_passwd = result.passwd
                logger.info { "Connected session_id=#{@session_id}, timeout=#{@timeout}, ping=#{@ping_interval}" }

                # Why 2 / 7 of the timeout?. If a binding sees no server response in this period it is required to
                # generate a ping request
                # if 2 periods go by without activity it is required to disconnect
                # so we are already more than half way through the session timeout
                # and we need to give ourselves time to reconnect to another server
                @ping_interval = @timeout * 2.0 / 7.0

                synchronize do
                    logger.debug { "Sending #{@pending_queue.length} queued packets" }
                    @session_state = :connected
                    @pending_queue.each { |p| send_packet(p) }
                    send_close_packet_if_necessary()
                end

                invoke_watch(@watcher,KeeperState::CONNECTED,nil,WatchEvent::NONE) if @watcher
            end
        end

        def send_session_connect() 
            req = Proto::ConnectRequest.new( :timeout => timeout )
            req.last_zxid_seen = @last_zxid_seen if @last_zxid_seen
            req.session_id =  @session_id if @session_id
            req.passwd = @session_passwd || ""
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

            case header.xid.to_i
            when -2
                ping_logger.debug { "Ping reply" }
                send_close_packet_if_necessary()
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

                if packet == nil && @close_packet
                    packet = @close_packet
                    @close_packet = nil
                    @session_state = :closing
                end

                logger.debug { "Packet reply: #{packet.inspect}" }

                if (packet.xid.to_i != header.xid.to_i)

                    # Treat this like a dropped connection, and then force the connection
                    # to be dropped. But wait for the connection to notify us before
                    # we actually update our keeper_state
                    invoke_response(*packet.error(:disconnected))
                    raise ProtocolError, "Bad XID. expected=#{packet.xid}, received=#{header.xid}"
                else
                    @last_zxid_seen = header.zxid

                    callback, error, response, watch_type =  packet.result(header.err.to_i)
                    invoke_response(callback, error, response, packet_io)

                    if (watch_type) 
                        @watches[watch_type][packet.path] << packet.watcher 
                        logger.debug { "Registered #{packet.watcher} for type=#{watch_type} at #{packet.path}" }
                    end
                    send_close_packet_if_necessary()
                end
            end

        end

        def process_watch_notification(state,path,event)

            watch_event = WatchEvent.fetch(event)
            watch_types = watch_event.watch_types()

            keeper_state = KeeperState.fetch(state)

            watches = watch_types.inject(Set.new()) do |result, watch_type|
                more_watches = @watches[watch_type].delete(path)
                result.merge(more_watches) if more_watches
                result
            end

            if watches.empty?
                logger.warn { "Received notification for unregistered watch #{state} #{path} #{event}" }
            end
            watches.each { | watch | invoke_watch(watch,keeper_state,path,watch_event) }      
        end

        def invoke_watch(watch,state,path,event)
            logger.debug { "Watch #{watch} triggered with #{state}, #{path}. #{event}" }
            if watch.respond_to?(:process_watch)
                callback = Proc.new() { |state,path,event| watch.process_watch(state,path,event) } 
            elsif watch.respond_to?(:call)
                callback = watch
            else
                logger.error("Bad watcher #{watch}")
            end
            @event_loop.invoke(callback,state,unchroot(path),event)
        end

        def clear_pending_queue(reason)
            synchronize do
                @session_state = reason
                @pending_queue.each  { |p| invoke_response(*p.error(reason)) }
                @pending_queue.clear()
                if @close_packet
                    invoke_response(*@close_packet.error(reason))
                    @close_packet = nil
                end
            end
        end

        def send_close_packet_if_necessary
            # We don't need to synchronize this because the creation of 
            # the close packet was synchronized and after that all
            # client requests are rejected
            # we can receive watch and ping notifications after this
            # but the server drops the connection as soon as this
            # packet is received
            if @pending_queue.empty? && @session_state == :connected && @close_packet
                logger.debug { "Sending close packet!" }
                send_packet(@close_packet)
            end
        end

        def invoke_response(callback,error,response,packet_io = nil)
            if callback
                result = if error
                             nil
                         elsif response.respond_to?(:read) && packet_io
                             response.read(packet_io)
                         elsif response
                             response
                         else
                             nil
                         end

                logger.debug { "Invoking response cb=#{callback} err=#{error} resp=#{result}"  }
                @event_loop.invoke(callback,error,result)
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
            logger.debug { "Queuing: #{packet.inspect}" }
            @pending_queue.push(packet)
            if @session_state == :connected
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

        class EventLoop 
            include Slf4r::Logger

            def initialize(client)
                @event_queue = Queue.new()

                @alive = true
                @event_thread  = Thread.new() do
                    logger.debug { "Starting event loop" }
                    Thread.current[:name] = "ZK::EventLoop #{self}"
                    Thread.current[CURRENT] = [ client ]
                    begin
                        pop_event_queue until dead?
                        logger.info { "Finished event loop" }
                    rescue Exception => ex
                        logger.error("Uncaught exception in event loop",ex)
                    end
                end
            end

            def dead?
                !@alive
            end

            # @api async_op
            def pop_event_queue
                #We're alive until we get a nil result from #stop
                logger.debug { "Popping event queue" }
                queued = @alive ? @event_queue.pop : nil
                if queued 
                    begin
                        callback,*args = queued
                        callback.call(*args)
                    rescue StandardError => ex
                        logger.error("Uncaught error in async callback", ex)
                    end
                else
                    @alive = false
                end
            end

            # @api session
            def invoke(*args)
                @event_queue.push(args)
            end

            def invoke_close(callback,*args)
                Thread.new do
                    @event_thread.join()
                    callback.call(*args)
                end
            end

            # @api session
            def stop
                @event_queue.push(nil)
                @event_thread.join()
            end

            # @api async_op
            def current?
                Thread.current == @event_thread
            end
        end
    end # Session
end
