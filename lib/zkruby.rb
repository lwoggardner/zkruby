require 'zk/logger'
require 'zk/bindata'
require 'generated/records'
require 'zk/enum'
require 'zk/protocol'
require 'zk/session'
require 'set'

class ZooKeeperError < StandardError
    attr_reader :err,:err_name

    def initialize(err)
        @err, @err_name = ZooKeeper::Errors.lookup(err)
    end
end

# A pure ruby implementation of the zk client library
#
# It implements the client side of the ZooKeeper TCP protocol directly rather
#   than calling the zk client libraries
#
# Advantages:
#   Rubyst API - with block is asynchronous, without block is synchronous
#   Ruby's threading model doesn't gel nicely with the ruby C library
#   Same library for JRuby or MRI
#   Connection bindings for pure ruby IO and eventmachine, or roll your own
#
# Disadvantages:
#   Duplicated code from Java/C libraries, particularly around herd effect protection
#   Maintain in parallel with breaking changes in protocol which are possibly more likely
#   than breaking changes in the client API
#   Probably not as optimised in terms of performance (but your client code is ruby anyway) 
#   Not production tested (yet- do you want to be the first?)  
#
# 
module ZooKeeper
    # Use the ZooKeeper version and last digit for us
    VERSION = "3.3.3.0"

    # Holds class constants representing available bindings
    BINDINGS = []

    # Provides a mapping between zk error codes and a human friendly name (symbol)
    # Can also be referenced as a constant
    class Errors
       include Enumeration
          enum :none, 0
          enum :system_error, -1
          enum :runtime_inconsistency, -2
          enum :data_inconsistency, -3
          enum :connection_lost, -4
          enum :marshalling_error, -5
          enum :unimplemented, -6
          enum :operation_timeout, -7
          enum :bad_arguments, -8
          enum :api_error, -100
          enum :no_node, -101
          enum :no_auth, -102
          enum :bad_version, -103
          enum :no_children_for_ephemerals, -108
          enum :node_exists, -110
          enum :not_empty, -111
          enum :session_expired, -112
          enum :invalid_callback, -113
          enum :invalid_acl, -114
          enum :auth_failed, -115
          enum :session_moved, -118

       # 
       # @param err either an integer or a symbol representing a zk error
       # @return <Array> [Fixnum,Symbol] code, name
       def self.lookup(err)
          error = self.get(err)
          return error.to_int, error.to_sym if error
          return -999, :unknown
       end
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
    # @param *perms list of permissions to combine, can be Perms constants or ints 
    def self.perms(*perms)
        perms.inject(0) { | result, perm | result = result | Perms.get(perm) }
    end

    # Convenience method to create a zk ID
    def self.id(scheme,id)
        Data::Identity.new(:scheme => scheme, :identity => id)
    end

    # Convenience method to create a zk ACL
    # See ::perms
    # eg
    #    ZK.acl(ZK.id("world","anyone"), ZK::Perms.DELETE, ZL::Perms.WRITE)
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

    # Returned by  asynchronous calls, various aliases for setting a block
    # to call if the request gets an error
    # eg 
    #    
    #    op = zk.stat("\apath") { ... }
    #    op.on_error { |err| puts ZooKeeper::Errors.get(err).to_sym }
    class QueuedOp
        def initialize(packet)
            @packet = packet
        end

        # @param &blk the error callback as a block
        def errback(&blk)
            @packet.errback=blk
        end

        # @param blk the error callback as a Proc
        def errback=(blk)
            @packet.errback=blk
        end

        alias :on_error :errback
    end

   # Main method for getting a client
   # TODO: take a block and do a bunch of zk things in it before calling close
   # start a fiber in the EM binding 
     def self.connect(addresses,options)
         if options.has_key?(:binding) 
            binding_type = options[:binding]
         else
            binding_type = BINDINGS.detect { |b| b.available? }
            raise ProtocolError "No available binding" unless binding_type
         end
         binding = binding_type.new()
         session = Session.new(binding,addresses,options)
         client = Client.new(binding)
         binding.start(session)
         client
     end

     class Client

        def initialize(binding)
            @binding = binding
        end

        def children(path,watch=nil,&blk)
            return synchronous_call(:children,path,watch) unless block_given?

            req = Proto::GetChildren2Request.new(:path => path, :watch => watch)
            queue_request(req,:get_children2,12,Proto::GetChildren2Response) do | response |
                blk.call(response.stat, response.children.to_a)
            end
        end

        CREATE_OPTS = { :sequential => 2, :ephemeral => 1 }
        def create(path,data,acl,*modeopts,&blk)
            return synchronous_call(:create,path,data,acl,*modeopts) unless block_given?
            
            flags = modeopts.inject(0) { |flags,opt| 
               raise ArgumentError, "Unknown create option #{opt}" unless CREATE_OPTS.has_key?(opt)
               flags | CREATE_OPTS[opt]
            }

            req = Proto::CreateRequest.new(:path => path, :data => data, :acl => acl, :flags => flags)
            queue_request(req,:create,1,Proto::CreateResponse) do | response |
                blk.call(response.path)
            end
        end

        # @return Array<Stat,String> [stat,data]
        def get(path,watch=nil,&blk)
            return synchronous_call(:get,path,watch) unless block_given?
            
            req = Proto::GetDataRequest.new(:path => path, :watch => watch)
            
            queue_request(req,:get,4,Proto::GetDataResponse) do | response |
                blk.call( response.stat, response.data.to_s)
            end
        end

        def exists(path,watch=nil,&blk)
            return synchronous_call(:exists,path,watch) unless block_given?

            req = Proto::ExistsRequest.new(:path => path, :watch => watch)
            queue_request(req,:exists,3,Proto::ExistsResponse,:exists,watch,ExistsPacket) do | response |
                blk.call( response.nil? ? nil : response.stat ) 
            end
        end
        alias :exists? :exists
        alias :stat :exists

        # Close the client
        def close(&blk)
            return synchronous_call(:close) unless block_given?
            @binding.close(&blk)
        end

        private
        def synchronous_call(method,*args)
            @binding.synchronous_call(self,method,*args)
        end

        def queue_request(*args,&blk)
            @binding.queue_request(*args,&blk)
        end

     end

end
