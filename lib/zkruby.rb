require 'set'
require 'slf4r'
require 'zk/bindata'
require 'zk/enum'
require 'zk/protocol'
require 'zk/session'
require 'jute/zookeeper'
require 'zk/multi'

# A pure ruby implementation of the zk client library
#
# It implements the client side of the ZooKeeper TCP protocol directly rather
#   than calling the zk client libraries
#
module ZooKeeper


    # Use the ZooKeeper version and last digit for us
    VERSION = "3.4.2.1"

    BINDINGS = []


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
    # @param [Perms] perms... list of permissions to combine, can be {Perms} constants, symbols or ints 
    # @return [Fixnum] integer representing the combined permission
    def self.perms(*perms)
        perms.inject(0) { | result, perm | result = result | Perms.get(perm) }
    end

    # Convenience method to create a zk Identity
    # @param [String] scheme
    # @param [String] identity
    # @return [Data::Identity] the encapsulated identity for the given scheme
    def self.id(scheme,id)
        Data::Identity.new(:scheme => scheme, :identity => id)
    end

    # Convenience method to create a zk ACL
    #    ZK.acl(ZK.id("world","anyone"), ZK::Perms.DELETE, ZL::Perms.WRITE)
    # @param [Data::Identity] id
    # @param [Perms] *perms list of permissions
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
    # @option options [Class]  :binding binding optional implementation class
    #    either {EventMachine::Binding} or {RubyIO::Binding} but normally autodiscovered
    # @option options [String] :chroot chroot path.
    #    All client calls will be made relative to this path 
    # @option options [Watcher] :watch the default watcher
    # @option options [String] :scheme the authentication scheme
    # @option options [String] :auth   the authentication credentials
    # @yieldparam [Client]
    # @return [Client] 
    def self.connect(addresses,options={},&block)
        if options.has_key?(:binding)
            binding_type = options[:binding]
            #TODO need to add to BINDINGS if not already there
            #TODO BINDINGS should be a class variable, not a constant
        else
            binding_type = BINDINGS.detect { |b| b.available? }
            raise ProtocolError,"No available binding" unless binding_type
        end
        binding = binding_type.new()
        session = Session.new(binding,addresses,options)
        client = Client.new(binding)
        binding.start(client,session)

        return client unless block_given?

        binding_type.context() do |storage|
            @binding_storage = storage
            storage.current[CURRENT] ||= []
            storage.current[CURRENT].push(client)
            begin
                block.call(client)
            ensure
                storage.current[CURRENT].pop
                client.close() unless session.closed?
            end
        end
    end

    # within the block supplied to {#connect} this will return the
    # current ZK client
    def self.current
        #We'd use if key? here if strand supported it
        @binding_storage.current[CURRENT].last if @binding_storage.current[CURRENT]
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


    # @abstract.
    class Watcher
        # @param [KeeperState] state representing the session state
        #    (:connected, :disconnected, :auth_failed, :session_expired)
        # @param [String] path the effected path
        # @param [WatchEvent] event the event that triggered the watch 
        def process_watch(state,path,event)
        end
    end
    
    #@private
    module Operations
        CREATE_OPTS = { :sequential => 2, :ephemeral => 1 }

        private
        def op_create(path,data,acl,*modeopts,&callback)
            return synchronous_call(:op_create,path,data,acl,*modeopts)[0] unless block_given?
            flags = modeopts.inject(0) { |flags,opt|
                raise ArgumentError, "Unknown create option #{ opt }" unless CREATE_OPTS.has_key?(opt)
                flags | CREATE_OPTS[opt]
            }
            path = session.chroot(path) 

            req = Proto::CreateRequest.new(:path => path, :data => data, :acl => acl, :flags => flags)
            queue_request(req,:create,1,Proto::CreateResponse) do | response |
                callback.call(session.unchroot(response.path)) if callback
            end

        end

        def op_set(path,data,version,&callback)
            return synchronous_call(:op_set,path,data,version)[0] unless block_given?
            path = session.chroot(path) 

            req = Proto::SetDataRequest.new(:path => path, :data => data, :version => version)
            queue_request(req,:set_data,5,Proto::SetDataResponse) do | response |
                callback.call( response.stat ) if callback
            end

        end

        def op_delete(path,version,&callback)
            return synchronous_call(:op_delete,path,version) unless block_given?
            path = session.chroot(path) 

            req = Proto::DeleteRequest.new(:path => path, :version => version)
            queue_request(req,:delete,2) do |response|
                callback.call() if callback
            end

        end

        def op_check(path,version,&callback)
            return synchronous_call(:op_check,path,version) unless block_given?
            path = session.chroot(path)
            req = Proto::CheckVersionRequest.new(:path => path, :version => version)
            queue_request(req,:check,13) do |response|
                callback.call() if callback
            end
        end

    end

    # Client API
    # 
    # All calls operate asynchronously or synchronously based on whether a block is supplied
    #
    # Without a block, requests are executed synchronously and either return results directly or raise
    # a {Error}
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

        include Operations
        # @api private
        # See {::ZooKeeper.connect}
        def initialize(binding)
            @binding = binding
        end

        # Session timeout, initially as supplied, but once connected is the negotiated
        # timeout with the server. 
        def timeout
            @binding.session.timeout
        end

        # The currently registered default watcher
        def watcher 
            @binding.session.watcher
        end

        # Assign the watcher to the session. This watcher will receive session connect/disconnect/expired
        # events as well as any path based watches registered to the API calls using the literal value "true"
        # @param [Watcher|Proc] watcher
        def watcher=(watcher)
            @binding.session.watcher=watcher
        end

        # Retrieve the list of children at the given path
        # @overload children(path,watch=nil)
        #    @param [String] path 
        #    @param [Watcher] if supplied sets a child watch on the given path
        #    @return [Data::Stat,Array<String>] stat,children stat of path and the list of child nodes
        #    @raise [Error] 
        # @overload children(path,watch=nil)
        #    @return [AsyncOp] asynchronous operation
        #    @yieldparam [Data::Stat]  stat current stat of path
        #    @yieldparam [Array<String>] children the list of child nodes at path 
        def children(path,watch=nil,&callback)
            return synchronous_call(:children,path,watch) unless block_given?
            path = chroot(path) 

            req = Proto::GetChildren2Request.new(:path => path, :watch => watch)
            queue_request(req,:get_children2,12,Proto::GetChildren2Response,:children,watch) do | response |
                callback.call(response.stat, response.children.to_a)
            end
        end

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
        def create(path,data,acl,*modeopts,&callback)
            op_create(path,data,acl,*modeopts,&callback)
        end

        # Retrieve data
        # @overload get(path,watch=nil)
        #   @param [String] path
        #   @param [Watcher] watch optional data watch to set on this path
        #   @return [Data::Stat,String] stat,data at path 
        #   @raise [Error]
        # @overload get(path,watch=nil)
        #   @return [AsyncOp] asynchronous operation
        #   @yieldparam [Data::Stat] stat Stat of the path
        #   @yieldparam [String] data Content at path
        def get(path,watch=nil,&blk)
            return synchronous_call(:get,path,watch) unless block_given?
            path = chroot(path) 

            req = Proto::GetDataRequest.new(:path => path, :watch => watch)

            queue_request(req,:get,4,Proto::GetDataResponse,:data,watch) do | response |
                blk.call( response.stat, response.data.to_s)
            end
        end

        # Retrieve the {Data::Stat} of a path, or nil if the path does not exist
        # @overload exists(path,watch=nil)
        #   @param [String] path
        #   @param [Watcher] wath optional exists watch to set on this path
        #   @return [Data::Stat] Stat of the path or nil if the path does not exist
        #   @raise [Error]
        # @overload exists(path,watch=nil)
        #   @return [AsyncOp] asynchronous operation
        #   @yieldparam [Data:Stat] stat Stat of the path or nil if the path did not exist
        def exists(path,watch=nil,&blk)
            return synchronous_call(:exists,path,watch)[0] unless block_given?
            path = chroot(path) 

            req = Proto::ExistsRequest.new(:path => path, :watch => watch)
            queue_request(req,:exists,3,Proto::ExistsResponse,:exists,watch,ExistsPacket) do | response |
                blk.call( response.nil? ? nil : response.stat )
            end
        end
        alias :exists? :exists
        alias :stat :exists

        # Delete path
        # @overload delete(path,version)
        #    @param [String] path
        #    @param [FixNum] version the expected version to be deleted (-1 to match any version)
        #    @return 
        #    @raise [Error]
        # @overload delete(path,version)
        #    @return [AsyncOp]
        #    @yield  [] callback invoked if delete is successful
        def delete(path,version,&callback)
            op_delete(path,version,&callback)
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
        def set(path,data,version,&callback)
            op_set(path,data,version,&callback)
        end

        # Get ACl
        # @overload get_acl(path)
        #    @param [String] path
        #    @return [Array<Data::ACL>] list of acls applying to path
        #    @raise [Error]
        # @overload get_acl(path)
        #   @return [AsyncOp] asynchronous operation
        #   @yieldparam [Array<Data::ACL>] list of acls applying to path
        def get_acl(path,&blk)
            return synchronous_call(:get_acl,path)[0] unless block_given?
            path = chroot(path) 

            req = Proto::GetACLRequest.new(:path => path)
            queue_request(req,:get_acl,6,Proto::GetACLResponse) do | response |
                blk.call( response.acl )
            end
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
        def set_acl(path,acl,version,&blk)
            return synchronous_call(:set_acl,acl,version)[0] unless block_given?
            path = chroot(path) 

            req = Proto::SetACLRequest.new(:path => path, :acl => acl, :version => version)
            queue_request(req,:set_acl,7,Proto::SetACLResponse) do | response |
                blk.call( response.stat )
            end

        end

        # Synchronise path between session and leader
        # @overload sync(path)
        #   @param [String] path
        #   @return [String] path
        #   @raise [Error]
        # @overload sync(path)
        #   @return [AsyncOp] asynchronous operation
        #   @yieldparam [String] path 
        def sync(path,&blk)
            return synchronous_call(:sync,path)[0] unless block_given?
            path = chroot(path) 
            req = Proto::SyncRequest.new(:path => path)
            queue_request(req,:sync,9,Proto::SyncResponse) do | response |
                blk.call(unchroot(response.path))
            end
        end

        # Close the session
        # @overload close()
        #    @raise [Error]
        # @overload close()
        #    @return [AsyncOp] asynchronous operation
        #    @yield [] callback invoked when session is closed 
        def close(&blk)
            return synchronous_call(:close) unless block_given?
            @binding.close(&blk)
        end

        # @api private
        # See {#transaction} 
        def multi(ops,&callback)
            return synchronous_call(:multi,ops) unless block_given?

            req = Proto::MultiRequest.new()

            ops.each do |op|
                req.requests << { :header => { :_type => op.opcode, :done => false, :err=> 0 }, :request => op.request }
            end

            req.requests << { :header => { :_type => -1 , :done => true, :err => -1 } }

            logger.debug("Multi #{req}")
            queue_request(req,:multi,14,Proto::MultiResponse) do |response|
                exception = nil  
                response.responses.each_with_index() do |multi_response,index|
                    next if multi_response.done?
                    op = ops[index]
                    if multi_response.header._type == -1
                        errcode = multi_response.header.err.to_i
                        if (errcode != 0)
                            exception = Error.lookup(errcode).exception("Transaction error for op ##{index} - #{op.op} (#{op.path})")
                            #TODO just raises the first exception
                            raise exception
                        end
                    else
                        callback_args = if multi_response.has_response? then [ multi_response.response ] else [] end
                        ops[index].callback.call(*callback_args)
                    end
                end
            end
        end

        # Perform multiple operations in a transaction
        # @overload transaction()
        #   @return [Transaction]
        # @overload transaction(&block)
        #  Execute the supplied block and commit the transaction (synchronously)
        #  @yieldparam [Transaction] txn
        def transaction(&block)
            txn = Transaction.new(self,session)
            return txn unless block_given?

            yield txn
            txn.commit
        end
        private

        def session
            @binding.session
        end

        def synchronous_call(method,*args)
            op = self.send(method,*args) do |*results|
                results 
            end
            op.backtrace = op.backtrace[2..-1] if op.backtrace

            op.value
        end

        def queue_request(*args,&blk)
            op = @binding.queue_request(*args,&blk)
            op.backtrace = caller[1..-1]
            op
        end

        def chroot(path)
            session.chroot(path)
        end

        def unchroot(path)
            session.unchroot(path)
        end

    end

    # Collects zookeeper operations to execute as a single transaction
    #
    # The builder methods {#create} {#delete} {#check} {#set} all take
    # an optional callback block that will be executed if the {#commit} succeeds.
    # 
    # If the transaction fails none of these callbacks will be executed.
    class Transaction
        include Operations
        #:nodoc
        def initialize(client,session)
            @client = client
            @session = session
            @ops = []
        end


        # Create a node
        #   @param [String] path the base name of the path to create
        #   @param [String] data the content to store at path
        #   @param [Data::ACL] acl the access control list to apply to the new node
        #   @param [Symbol,...] modeopts combination of :sequential, :emphemeral
        #   @yieldparam [String] path the created path
        def create(path,data,acl,*modeopts,&callback)
            op_create(path,data,acl,*modeopts,&callback)
        end
        
        # Delete path
        #    @param [String] path
        #    @param [FixNum] version the expected version to be deleted (-1 to match any version)
        #    @yield  [] callback invoked if delete is successful
        def delete(path,version,&callback)
            op_delete(path,version,&callback)
        end

        # Set Data
        #    @param [String] path
        #    @param [String] data content to set at path
        #    @param [Fixnum] version expected current version at path or -1 for any version
        #    @yieldparam [Data::Stat] stat new stat of path
        def set(path,data,version,&callback)
            op_set(path,data,version,&callback)
        end

        # Check Version
        def check(path,version,&callback)
            op_check(path,version,&callback)
        end

        # Commit the transaction
        # @overload commit()
        #   Synchronously commit the transaction
        #   @raise [Error] if the transaction failed
        # @overload commit(&callback)
        #   @return [AsyncOp] captures the result of the asynchronous operation
        #   @yield [] callback invoked if transaction is successful
        def commit(&callback)
            op = client.multi(ops,&callback)
        end

        private
        attr_reader :client, :session, :ops
        
        def queue_request(request,op,opcode,response=nil,&callback)
            ops << Operation.new(op,opcode,request,response,callback)
        end

        #Just ensure we have an empty block
        def synchronous_call(method,*args)
            self.send(method,*args) { |*results| results  }
        end
    end



end

# Shorthand
ZK=ZooKeeper

