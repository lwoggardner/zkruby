module ZooKeeper

  # Represents failure mode of ZooKeeper operations
  # They are raised and rescued in the standard ways (much like ruby's Errno)
  # @example
  #   begin
  #       zk.create(...)
  #   rescue ZK::Error::NODE_EXISTS
  #       ....
  #   rescue ZK::Error::CONNECTION_LOST
  #   end
  #
  class Error < StandardError
    include Enumeration
    enum :none, 0
    enum :system_error,  (-1)
    enum :runtime_inconsistency, (-2)
    enum :data_inconsistency, (-3)
    enum :connection_lost, (-4)
    enum :marshalling_error, (-5)
    enum :unimplemented, (-6)
    enum :operation_timeout, (-7)
    enum :bad_arguments, (-8)
    enum :api_error, (-100)
    enum :no_node, (-101)
    enum :no_auth, (-102)
    enum :bad_version, (-103)
    enum :no_children_for_ephemerals, (-108)
    enum :node_exists, (-110)
    enum :not_empty, (-111)
    enum :session_expired, (-112)
    enum :invalid_callback, (-113)
    enum :invalid_acl, (-114)
    enum :auth_failed, (-115)
    enum :session_moved, (-118)
    enum :unknown, (-999)
  end

  # Permission constants
  class Perms
    include Enumeration
    enum :read, 1 << 0
    enum :write, 1 << 1
    enum :create, 1 << 2
    enum :delete, 1 << 3
    enum :admin, 1 << 4
    enum :all, READ | WRITE | CREATE | DELETE | ADMIN
  end


  # Combine permissions constants
  # @param [Perms...] perms list of permissions to combine, can be {Perms} constants, symbols or ints
  # @return [Fixnum] integer representing the combined permission
  def self.perms(*perms)
    perms.inject(0) { | result, perm | result = result | Perms.get(perm) }
  end

  # Convenience method to create a zk Identity
  # @param [String] scheme
  # @param [String] id
  # @return [Data::Identity] the encapsulated identity for the given scheme
  def self.id(scheme,id)
    Data::Identity.new(:scheme => scheme, :identity => id)
  end

  # Convenience method to create a zk ACL
  #    ZK.acl(ZK.id("world","anyone"), ZK::Perms.DELETE, ZL::Perms.WRITE)
  # @param [Data::Identity] id
  # @param [Perms...] perms list of permissions
  # @return [Data::ACL] an access control list
  # @see #perms
  #
  #
  def self.acl(id,*perms)
    Data::ACL.new( :identity => id, :perms => self.perms(*perms) )
  end

  #
  # The Anyone ID
  ANYONE_ID_UNSAFE = Data::Identity.new(:scheme => "world", :identity => "anyone")

  # Represents the set of auth ids for the current connection
  AUTH_IDS = Data::Identity.new(:scheme => "auth")

  OPEN_ACL_UNSAFE = [ acl(ANYONE_ID_UNSAFE, Perms::ALL) ]
  CREATOR_ALL_ACL = [ acl(AUTH_IDS, Perms::ALL) ]
  READ_ACL_UNSAFE = [ acl(ANYONE_ID_UNSAFE, Perms::READ) ]

  # The Anyone ID
  ID_ANYONE_UNSAFE = ANYONE_ID_UNSAFE

  # Represents the set of auth ids for the current connection
  ID_USE_AUTHS = AUTH_IDS

  ACL_OPEN_UNSAFE = OPEN_ACL_UNSAFE
  ACL_CREATOR_ALL = CREATOR_ALL_ACL
  ACL_READ_UNSAFE = READ_ACL_UNSAFE


  def self.seq_to_path(path,id)
    format("%s%010d",path,id)
  end

  def self.path_to_seq(path)
    matches = /^(.*)(\d{10})$/.match(path)
    matches ? [matches[1],matches[2].to_i] : [path,nil]
  end

  CURRENT = :zookeeper_current
  # Main method for connecting to a client
  # @param addresses [Array<String>] list of host:port for the ZK cluster as Array or comma separated String
  # @option options [String] :chroot chroot path.
  #    All client calls will be made relative to this path
  # @option options [Watcher] :watch the default watcher
  # @option options [String] :scheme the authentication scheme
  # @option options [String] :auth   the authentication credentials
  # @yieldparam [Client]
  # @return [Client]
  def self.connect(addresses,options={},&block)

    session = Session.new(addresses,options)
    client = Client.new(session,options)

    session.start(client)

    return client unless block_given?

    storage = Thread.current[CURRENT] ||= []
    storage.push(client)
    begin
      yield client
    ensure
      storage.pop
      #TODO this will throw an exception if expired
      client.close()
    end
  end

  # within the block supplied to {ZooKeeper.connect} this will return the
  # current ZK client
  def self.current
    #We'd use if key? here if strand supported it
    Thread.current[CURRENT].last if Thread.current.key?(CURRENT)
  end

  # Allow ZK a chance to send its data/ping
  def self.pass
    Thread.pass
  end

  class WatchEvent
    attr_reader :watch_types
    def initialize(watch_types)
      @watch_types = watch_types
    end

    include Enumeration
    enum :none,(-1),[]
    enum :node_created, 1, [ :data, :exists ]
    enum :node_deleted, 2, [ :data, :children ]
    enum :node_data_changed, 3, [ :data, :exists ]
    enum :node_children_changed, 4, [ :children ]
  end

  class KeeperState
    include Enumeration

    enum :disconnected, 0
    enum :connected, 3
    enum :auth_failed, 4
    enum :expired, (-112)
  end


  # @abstract
  # The watch interface
  class Watcher
    # @param [KeeperState] state representing the session state
    #    (:connected, :disconnected, :auth_failed, :session_expired)
    # @param [String] path the effected path
    # @param [WatchEvent] event the event that triggered the watch
    def process_watch(state,path,event)
    end
  end

  # Operations that are available to call directly in #{Client} or as part of a {Transaction}
  module TransactionOps
    CREATE_OPTS = { :sequential => 2, :ephemeral => 1 }

    # Create a node
    # @overload create(path,data,acl,*modeopts)
    #   Synchronous style
    #   @param [String] path the base name of the path to create
    #   @param [String] data the content to store at path
    #   @param [Data::ACL] acl the access control list to apply to the new node
    #   @param [Symbol,...] modeopts combination of :sequential, :emphemeral
    #   @return [String] the created path, only different if :sequential is requested
    #   @raise [Error]
    # @overload create(path,data,acl,*modeopts)
    #   @return [AsyncOp] asynchronous operation
    #   @yieldparam [String] path the created path
    def create(path, data, acl, *modeopts, &callback)
      flags = modeopts.inject(0) { |flags,opt|
        raise ArgumentError, "Unknown create option #{ opt }" unless CREATE_OPTS.has_key?(opt)
        flags | CREATE_OPTS[opt]
      }
      queue_request(:create, path, nil, {data: data, acl: acl, flags: flags}, callback) { |response| unchroot(response.path) }
    end

    # Delete path
    # @overload delete(path,version)
    #    @param [String] path
    #    @param [FixNum] version the expected version to be deleted (-1 to match any version)
    #    @return [void]
    #    @raise [Error]
    # @overload delete(path,version)
    #    @return [AsyncOp]
    #    @yield  [] callback invoked if delete is successful
    def delete( path, version, &callback )
      queue_request( :delete, path, nil, {version: version}, callback ) { true }
    end

    # Set Data
    # @overload set(path,data,version)
    #    @param [String] path
    #    @param [String] data content to set at path
    #    @param [Fixnum] version expected current version at path
    #    @return [Data::Stat] new stat of path (ie new version)
    #    @raise [Error]
    # @overload set(path,data,version)
    #    @return [AsyncOp] asynchronous operation
    #    @yieldparam [Data::Stat] stat new stat of path
    def set( path, data, version, &callback)
      queue_request(:set_data, path, nil, {data: data, version: version}, callback) { |response| response.stat }
    end

    # Check Version
    # @overload check(path,version)
    #   @param [String] path
    #   @param [Fixnum] version expected current version
    #   @return [Boolean] if version matches
    #   @raise Error::BAD_VERSION if version does not match
    # @overload check(path,version)
    #   @return [AsyncOp] asynchronous operation
    #   @yield [] callback invoked if version matches
    def check( path, version, &callback )
      queue_request( :check, path, nil, {version: version}, callback ) { |response| true }
    end
  end

  # Client API
  #
  # All calls operate asynchronously or synchronously based on whether a block is supplied
  #
  # Without a block, requests are executed synchronously and either return results directly or raise
  # an {Error}
  #
  # With a block, the request returns immediately with a {AsyncOp}. When the server responds the
  # block is passed the results. Errors will be sent to an error callback if registered on the {AsyncOp}
  #
  # Requests that take a watch argument can be passed either...
  #   * An object that quacks like a {Watcher}
  #   * A Proc will be invoked with arguments state, path, event
  #   * The literal value "true" refers to the default watcher registered with the session
  #
  # Registered watches will be fired exactly once for a given path with either the expected event
  # or with state :expired and event :none when the session is finalised
  class Client

    include TransactionOps

    #@!visibility private
    def initialize(session,options)
      @session = session
      @chroot = options.fetch(:chroot,"").chomp("/")
      normalize_watch(options.fetch(:watch,nil))
    end

    # Session timeout, initially as supplied, but once connected is the negotiated
    # timeout with the server.
    def timeout
      session.session_timeout
    end

    # The currently registered default watcher
    def watcher
      session.watcher
    end

    # Assign the watcher to the session. This watcher will receive session connect/disconnect/expired
    # events as well as any path based watches registered to the API calls using the literal value "true"
    # @param [Watcher,#process_watch,Proc] new_watcher
    def watcher=(new_watcher)
      session.watcher = normalize_watch(new_watcher)
    end

    # Retrieve the list of children at the given path
    # @overload children(path,watch=nil)
    #    @param [String] path
    #    @param [Watcher,#process_watch,Proc] if supplied sets a child watch on the given path
    #    @return [Data::Stat,Array<String>] stat,children stat of path and the list of child nodes
    #    @raise [Error]
    # @overload children(path,watch=nil)
    #    @return [AsyncOp] asynchronous operation
    #    @yieldparam [Data::Stat]  stat current stat of path
    #    @yieldparam [Array<String>] children the list of child nodes at path
    def children(path,watch=nil,&callback)
      queue_request(:get_children2, path, watch, {}, callback) { |response| [ response.stat, response.children.to_a ] }
    end

    # Retrieve data
    # @overload get(path,watch=nil)
    #   @param [String] path
    #   @param [Watcher,#process_watch,Proc] watch optional data watch to set on this path
    #   @return [Data::Stat,String] stat,data at path
    #   @raise [Error]
    # @overload get(path,watch=nil)
    #   @return [AsyncOp] asynchronous operation
    #   @yieldparam [Data::Stat] stat Stat of the path
    #   @yieldparam [String] data Content at path
    def get(path,watch=nil,&callback)
      queue_request(:get, path, watch, {}, callback) { |response| [ response.stat, response.data.value ] }
    end

    # Retrieve the {Data::Stat} of a path, or nil if the path does not exist
    # @overload exists(path,watch=nil)
    #   @param [String] path
    #   @param [Watcher,#process_watch,Proc] watch optional exists watch to set on this path
    #   @return [Data::Stat] Stat of the path or nil if the path does not exist
    #   @raise [Error]
    # @overload exists(path,watch=nil)
    #   @return [AsyncOp] asynchronous operation
    #   @yieldparam [Data:Stat] stat Stat of the path or nil if the path did not exist
    def exists(path,watch=nil,&callback)
      queue_request(:exists, path, watch, {}, callback) { |response| response && response.stat }
    end
    alias :exists? :exists
    alias :stat :exists

    # Get ACl
    # @overload get_acl(path)
    #    @param [String] path
    #    @return [Array<Data::ACL>] list of acls applying to path
    #    @raise [Error]
    # @overload get_acl(path)
    #   @return [AsyncOp] asynchronous operation
    #   @yieldparam [Array<Data::ACL>] list of acls applying to path
    def get_acl(path,&callback)
      queue_request(:get_acl,path,nil, {},callback) { |response| response.acl }
    end

    # Set ACL
    # @overload set_acl(path,acl,version)
    #    @param [String] path
    #    @param [Array<Data::ACL>] acl list of acls for path
    #    @param [Fixnum] version expected current version
    #    @return Data::Stat new stat for path if successful
    #    @raise [Error]
    # @overload set_acl(path,acl,version)
    #    @return [AsyncOp] asynchronous operation
    #    @yieldparam [Data::Stat] new stat for path if successful
    def set_acl(path,acl,version,&callback)
      queue_request(:set_acl, path, nil, {acl: acl, version: version}, callback) { |response| response.stat }
    end

    # Synchronise path between session and leader
    # @overload sync(path)
    #   @param [String] path
    #   @return [String] path
    #   @raise [Error]
    # @overload sync(path)
    #   @return [AsyncOp] asynchronous operation
    #   @yieldparam [String] path
    def sync(path, &callback)
      queue_request(:sync, path, nil, {}, callback) { |response| unchroot(response.path) }
    end

    # Close the session
    # @overload close()
    #    @raise [Error]
    # @overload close()
    #    @return [AsyncOp] asynchronous operation
    #    @yield [] callback invoked when session is closed
    def close(&callback)
      if callback
        session.close(&callback)
      else
        session.close(){}.value
      end
    end

    # Perform multiple operations in a transaction
    # @overload transaction()
    #   @return [Transaction]
    # @overload transaction(&block)
    #  Execute the supplied block and commit the transaction (synchronously)
    #  @yieldparam [Transaction] txn
    def transaction(&block)
      txn = Transaction.new(self)
      return txn unless block_given?

      yield txn
      txn.commit
    end

    # @!visibility private
    # See {#transaction}
    def multi(ops,&callback)

      op_data = ops.collect do |op|
        [ op.op_type, op.op_data ]
      end

      queue_request( :multi, nil, nil, op_data, callback) do |response|
        response.responses.each_with_index.collect do |multi_response,index|
          next if multi_response.done?
          op = ops[index]

          if multi_response.error?
            errcode = multi_response.errcode
            raise Error.lookup(errcode), "Transaction error for op #{index} - #{op.op_type} (#{op.path})" if (errcode != 0)
          else
            op.result(multi_response.response)
          end
        end
      end
    end


    #@!visibility private
    def chroot(path)
      return @chroot if path == "/"
      return @chroot + path
    end

    #@!visibility private
    def unchroot(path)
      return path unless path
      path.slice(@chroot.length..-1)
    end

    #@!visibility private
    def result(response, callback, response_proc)
      response_results = response_proc.call(response) if response
      callback ? callback.call(*response_results) : response_results
    end

    private
    attr_reader :session

    def queue_request(op_type, path, watch, op_data, callback, &response_proc)
      op_data.merge!(path: chroot(path)) if path
      op = session.request(op_type, op_data, normalize_watch(watch)) { |response| result(response,callback,response_proc) }
      op.backtrace = caller[1..-1]

      # if a callback is provided return the async op, otherwise block and wait for the value
      callback ?  op : op.value
    end

    def normalize_watch(watch)
      case
      when watch == true
        self.watcher
      when !watch
        watch
      when watch.respond_to?(:process_watch)
        Proc.new() { |state, path, event| watch.process_watch(state,unchroot(path),event) }
      when watch.respond_to?(:call)
        Proc.new() { |state, path, event| watch.call(state, unchroot(path), event) }
      else
        raise ArgumentError, "Bad watcher #{watch}"
      end
    end
  end

  # Collects zookeeper operations to execute as a single transaction
  #
  # The builder methods {#create} {#delete} {#check} {#set} all take
  # an optional callback block that will be executed if the {#commit} succeeds.
  #
  # If the transaction fails none of these callbacks will be executed.
  class Transaction

    # @!visibility private
    class MultiOp
      attr_reader :op_type, :op_data
      def initialize(op_type, op_data, &result)
        @op_type,@op_data = op_type,op_data
        @result_proc = result
      end

      def result(response)
        @result_proc.call(response)
      end

      def path()
        op_data[:path]
      end
    end

    include TransactionOps
    #@!visibility private
    def initialize(client)
      @client = client
      @ops = []
    end

    # Commit the transaction
    # @overload commit()
    #   Synchronously commit the transaction
    #   @raise [Error] if the transaction failed
    #   @return [Array<Object>] results of the supplied operations
    # @overload commit(&callback)
    #   @return [AsyncOp] captures the result of the asynchronous operation
    #   @yield [Array<Object>] callback invoked with operation results if transaction is successful
    def commit(&callback)
      op = client.multi(ops,&callback)
    end

    private
    attr_reader :client, :ops

    def queue_request( op_type, path, watch, op_data, callback, &response_proc)
      #note watch is always nil for multi operations, but signature of queue_request is kept
      op_data.merge!(path: chroot(path)) if path
      ops << MultiOp.new(op_type, op_data) { |response| client.result(response,callback,response_proc) }
    end

    def chroot(path)
      client.chroot(path)
    end

    def unchroot(path)
      client.unchroot(path)
    end
  end

end

