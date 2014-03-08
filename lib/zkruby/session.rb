require 'set'
require 'thread'
require 'monitor'
require 'zkruby/async_op'
require 'zkruby/operations'
module ZooKeeper
  class SessionError < IOError;end

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

      @ping_logger = Slf4r::LoggerFacade.new("ZooKeeper::Session::Ping")
    end

    # @api zookeeper
    # See {ZooKeeper.connect}
    def start(client)
      raise SessionError, "Already started!" unless @session_state.nil?
      @session_state = :disconnected
      @disconnect_time = Time.now
      logger.debug {"Starting new zookeeper client session for #{client}"}

      @event_loop = EventLoop.new(client)

      Thread.new {
        Thread.current[:name] = "ZK::Session #{self}"
        session_loop()
      }
    end


    # @api client
    def request(op_type,op_data, watch, &callback)
      # @event_loop.async_op(callback) do ....
      AsyncOp.new(@event_loop,callback) do |op|
        queue_request(op_type,op_data,watch) do |error,response|
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

    attr_reader :session_timeout
    private
    attr_reader :ping_interval
    attr_reader :ping_logger
    attr_reader :connect_timeout
    attr_reader :record_io
    attr_reader :connection_factory

    def active?
      [:connected,:disconnected].include?(@session_state)
    end

    def connected?()
      @session_state == :connected
    end

    def closing?()
      @session_state == :closing
    end

    def calculate_timeouts(timeout)
      # Why 2 / 7 of the timeout?. If a binding sees no server response in this period it is required to
      # generate a ping request
      # if 2 periods go by without activity it is required to disconnect
      # so we are already more than half way through the session timeout
      # and we need to give ourselves time to reconnect to another server
      @session_timeout = timeout
      @ping_interval = timeout * 2.0 / 7.0
      @connect_timeout = timeout / 2.0
    end

    def queue_request(op_type,op_data,watch,&callback)
      synchronize do
        raise SessionError, "Client closed #{@client_state}" unless @client_state == :ready
        raise Error::SESSION_EXPIRED, "Session has expired #{@session_state}" unless active?

        xid = next_xid

        operation = OperationType.lookup(op_type).create(xid,op_data,watch,callback)

        queue_operation(operation)
      end
    end

    def close_session(&callback)
      synchronize do
        if @client_state == :ready
          if active?
            # we keep the requested block in a close operation but we don't send it
            # until we've received all pending reponses
            @close_operation = OperationType::CLOSE.create(next_xid(),nil,nil,callback)

            # but we can force a response by sending a ping
            send_ping()

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
      calculate_timeouts( options.fetch(:timeout,DEFAULT_TIMEOUT) )
      @max_connect_delay = options.fetch(:connect_delay,DEFAULT_CONNECT_DELAY)
      @scheme = options.fetch(:scheme,nil)
      @auth = options.fetch(:auth,nil)
      @connection_factory = options.fetch(:connection_factory,ZooKeeperBinding)
    end

    def session_loop
      loop do
        @record_io = connect()

        prime_connection()

        read_loop()

        break unless active?

        #This delay is to prevent flooding of a new server when one goes down
        delay = rand() * @max_connect_delay
        sleep(delay)
      end

      logger.debug {"Session #{self} complete" }
    rescue Exception => ex
      logger.error("Exception in session loop: #{ex.message}", ex)
      session_expired(:system_error)
    end

    def connect()
      host,port = @addresses.shift
      @addresses.push([host,port])

      logger.debug { "Connecting id=#{@session_id} to #{host}:#{port} with timeout=#{connect_timeout} #{connection_factory.inspect}" }
      connection_factory.connect(host,port,connect_timeout)
    end

    def read_loop()

      while connected?

        unless record_io.ready?(ping_interval)
          synchronize { send_ping() }
          unless record_io.ready?(ping_interval)
            logger.warn { "No response to ping" }
            break
          end
        end

        # We won't get here unless there is an available operation to consume
        process_reply()
      end
      record_io.close()
    ensure
      disconnected()
    end


    def prime_connection()
      synchronize do
        send_session_connect()
        send_auth_data()
        send_reset_watches()
      end

      record_io.ready?(ping_interval) && complete_connection()
    end

    def disconnected()
      logger.info { "Disconnected id=#{@session_id}, keeper=:#{@session_state}, client=:#{@client_state}" }

      # We keep trying to reconnect until the session expiration time is reached
      time_since_first_disconnect = (Time.now - @disconnect_time)

      if closing?
        #We were expecting this disconnect
        session_expired(:closed)
      elsif connected?
        #first disconnect
        @disconnect_time = Time.now
        clear_pending_queue(:disconnected)
        invoke_watch(watcher,KeeperState::DISCONNECTED,nil,WatchEvent::NONE) if watcher
        #@record_io = nil
      elsif Time.now - @disconnect_time > session_timeout
        session_expired(:expired)
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

    def complete_connection()
      result = record_io.read(Proto::ConnectResponse)
      if (result.time_out <= 0)
        #We're dead!
        session_expired()
      else
        calculate_timeouts(result.time_out.to_f / 1000.0)
        @session_id = result.session_id
        @session_passwd = result.passwd
        logger.info { "Connected session_id=#{@session_id}, timeout=#{session_timeout}, ping=#{ping_interval}" }

        synchronize do
          logger.debug { "Sending #{@pending_queue.length} queued operations" }
          @session_state = :connected
          @pending_queue.each { |p| send_operation(p) }
          send_close_operation_if_necessary()
        end

        invoke_watch(@watcher,KeeperState::CONNECTED,nil,WatchEvent::NONE) if @watcher
      end
    end
    def process_reply()
      header = record_io.read(Proto::ReplyHeader)

      case header.xid.to_i
      when -2
        ping_logger.debug { "Ping reply" }
        send_close_operation_if_necessary()
      when -4
        logger.debug { "Auth reply" }
        session_expired(:auth_failed) unless header.err.to_i == 0
      when -1
        #Watch notification
        event = record_io.read(Proto::WatcherEvent)
        logger.debug { "Watch notification #{event.inspect} " }
        process_watch_notification(event.state.to_i,event.path,event._type.to_i)
      when -8
        #Reset watch reply
        logger.debug { "SetWatch reply"}
        #TODO If error, send :disconnected to all watches
      else
        # A normal operation reply. They should come in the order we sent them
        # so we just match it to the operation at the front of the queue
        operation = synchronize { @pending_queue.shift }

        if operation.nil? && @close_operation
          operation = @close_operation
          @close_operation = nil
          @session_state = :closing
        end

        logger.debug { "Operation reply: #{operation}" }

        if (operation.xid.to_i != header.xid.to_i)

          # Treat this like a dropped connection, and then force the connection
          # to be dropped. But wait for the connection to notify us before
          # we actually update our keeper_state
          invoke_response(*operation.error(:disconnected))
          raise SessionError, "Bad XID. expected=#{operation.xid}, received=#{header.xid}"
        else
          @last_zxid_seen = header.zxid

          callback, error, response_class, watch_type =  operation.result(header.err.to_i)
          response = record_io.read(response_class) if response_class
          invoke_response(callback, error, response)

          if (watch_type)
            @watches[watch_type][operation.path] << operation.watcher
            logger.debug { "Registered #{operation.watcher} for type=#{watch_type} at #{operation.path}" }
          end
          send_close_operation_if_necessary()
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
      @event_loop.invoke(watch,state,path,event)
    end

    def clear_pending_queue(reason)
      synchronize do
        #TODO: reason MUST be an error code! - we should use the constants not the symbols
        #although they are still resolved at runtime
        #ie :connection_lost, :session_expired, :system_error, :auth_failed
        #Symboles are probably safer actually because otherwise we get a syntax error
        @session_state = reason
        @pending_queue.each  { |p| invoke_response(*p.error(reason)) }
        @pending_queue.clear()
        if @close_operation
          invoke_response(*@close_operation.error(reason))
          @close_operation = nil
        end
      end
    end

    def send_session_connect()
      req = Proto::ConnectRequest.new( :timeout => session_timeout )
      req.last_zxid_seen = @last_zxid_seen if @last_zxid_seen
      req.session_id =  @session_id if @session_id
      req.passwd = @session_passwd || ""
      record_io.write(req)
    end

    def send_auth_data()
      send_operation( OperationType::AUTH.create(-4, { :scheme => @scheme, :auth => @auth }, nil, nil)) if @scheme
    end

    # Watches are dropped on disconnect, we reset them here
    # dropping connections can be a good way of cleaning up on the server side
    # #TODO If watch reset is disabled the watches will be notified of connection loss
    # otherwise they will be seemlessly re-added
    # This way a watch is only ever triggered exactly once
    def send_reset_watches()
      unless watches[:children].empty? && watches[:data].empty? && watches[:exists].empty?
        op_data = { relative_zxid: @last_zxid_seen,
          data_watches: watches[:data].keys,
          child_watches: watches[:children].keys,
          exist_watches: watches[:exists].keys }

        send_operation( OperationType::SET_WATCHES.create(-8,op_data,nil))
      end
    end

    def send_ping()
      ping_logger.debug { "Ping send" }
      send_operation (OperationType::PING.create(-2,nil,nil,nil))
    end


    def send_close_operation_if_necessary
      # We don't need to synchronize this because the creation of
      # the close operation was synchronized and after that all
      # client requests are rejected
      # we can receive watch and ping notifications after this
      # but the server drops the connection as soon as this
      # operation is received
      if @pending_queue.empty? && connected? && @close_operation
        logger.debug { "Sending close operation!" }
        send_operation(@close_operation)
      end
    end

    def invoke_response(callback,error,response)
      if callback
        result = error ? nil : response
        logger.debug { "Invoking response cb=#{callback} err=#{error} resp=#{result}"  }
        @event_loop.invoke(callback,error,result)
      end
    end

    def queue_operation(operation)
      logger.debug { "Queuing: #{operation.inspect}" }
      @pending_queue.push(operation)
      send_operation(operation) if connected?
    end

    def next_xid
      @xid += 1
    end

    def send_operation(operation)
      record_io.write(operation.header, operation.request)
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
            logger.debug { "Invoking event #{callback}" }
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
        logger.debug { "Stopping event loop" }
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
