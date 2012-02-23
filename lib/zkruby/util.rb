module ZooKeeper

    class Client

        # Recursive make path
        #
        # This will send parallel creates for ALL nodes up to the root
        # and then ignore any NODE_EXISTS errors.
        #
        # You generally want to call this after receiving a NO_NODE error from a 
        # simple {#create}
        #
        # @param [String] path
        # @param [Data::ACL] acl the access control list to apply to any created nodes
        # @return 
        # @raise [Error]
        def mkpath(path,acl=ZK::ACL_OPEN_UNSAFE,&callback)

            return synchronous_call(:mkpath,path,acl) unless block_given?

            connection_lost = false

            path_comp = path.split("/")

            # we only care about the result of the final create op
            last_op = nil

            (1-path_comp.length..-1).each do |i|

                sub_path = path_comp[0..i].join("/")

                op = create(sub_path,"",acl) { if i == -1 then callback.call() else :true end }

                if i == -1
                    op.on_error(ZK::Error::NODE_EXISTS) { callback.call() }
                    op.on_error(ZK::Error::CONNECTION_LOST, ZK::Error::NO_NODE) { |ex|
                        connection_lost = true if ZK::Error::CONNECTION_LOST === ex
                        raise ex unless connection_lost
                        mkpath(path,acl) 
                        callback.call()
                    }
                    last_op = op
                else
                    op.on_error(ZK::Error::CONNECTION_LOST) { connection_lost = true }
                    op.on_error(ZK::Error::NODE_EXISTS) { }
                    op.on_error() { |err| logger.debug { "mkpath error creating #{sub_path}, #{err}" } }
                end
        end

        return WrappedOp.new(last_op)
    end

    # Recursive delete
    #
    # Although this method itself can be called synchronously all the zk activity
    # is asynchronous, ie all subnodes are removed in parallel
    #
    # Will retry on connection loss, or if some other activity is adding nodes in parallel.
    # If you get a session expiry in the middle of this you will end up with a
    # partially completed recursive delete. In all other circumstances it will eventually
    # complete.
    #
    # @overload rmpath(path,version)
    #    @param [String] path this node and all its children will be recursively deleted
    #    @param [FixNum] version the expected version to be deleted (-1 to match any version).
    #         Only applies to the top level path
    #    @return 
    #    @raise [Error]
    # @overload rmpath(path,version)
    #    @return [AsyncOp]
    #    @yield  [] callback invoked if delete is successful
    def rmpath(path,version = -1, &callback)

        return synchronous_call(:rmpath,path,version) unless block_given?

        del_op = delete(path,version) { callback.call() }

        del_op.on_error(ZK::Error::NO_NODE) { callback.call() }

        del_op.on_error(ZK::Error::CONNECTION_LOST) { del_op.try_again() }

        del_op.on_error(ZK::Error::NOT_EMPTY) do 

            stat, child_list = children(path)

            unless child_list.empty?
                
                child_ops = {}
                child_list.each do |child| 
                    child_path = "#{path}/#{child}"

                    rm_op = rmpath(child_path,-1) { :success }

                    rm_op.on_error { |err| child_results[child_path] = err }

                    child_ops[child_path] = rm_op
                    ZK.pass
                end

                # Wait until all our children are done (or error'd)
                child_ops.each { |child_path,op| op.value }
                rmpath(path,version)
            end
        end

        return WrappedOp.new(del_op)
    end
end #Client

class WrappedOp < AsyncOp
    #TODO you can't retry a wrapped op
    def initialize(delegate_op)
        @delegate_op = delegate_op
    end

    private
    def wait_value()
        begin  
            @delegate_op.value()
        rescue StandardError > err
            @errback ? @errback.call(err) : raise
        end
    end
end

end #ZooKeeper
