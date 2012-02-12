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

                op.errback do |err|
                    if i == -1
                        if ZK::Error::NODE_EXISTS === err
                            callback.call()
                        elsif ZK::Error::CONNECTION_LOST === err || ( ZK::Error::NO_NODE && connection_lost )
                            # try again
                            mkpath(path,acl)
                            callback.call()
                        else 
                            raise err 
                        end
                    elsif ZK::Error::CONNECTION_LOST === err
                        connection_lost = true
                        :connection_lost
                    else
                        # we don't care about any other errors, but we will log them
                        logger.warn { "Error creating #{sub_path}, #{err}" }
                    end
                end
                last_op = op if (i == -1)
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

            del_op.errback do |err|
                # we don't leave this method unless we get an exception
                # or have completed and called the callback
                case err
                when ZK::Error::NO_NODE
                when ZK::Error::CONNECTION_LOST
                    rmpath(path,version)
                when ZK::Error::NOT_EMPTY

                    stat, child_list = children(path)

                    unless child_list.empty?
                        child_ops = {}
                        child_list.each do |child| 
                            child_path = "#{path}/#{child}"

                            rm_op = rmpath(child_path,-1) { :success }

                            rm_op.errback { |err| child_results[child_path] = err }

                            child_ops[child_path] = rm_op
                            ZK.pass
                        end

                        # Wait until all our children are done (or error'd)
                        child_ops.each { |child_path,op| op.value }
                        rmpath(path,version)
                    end
                else
                    raise err
                end
                callback.call()
            end

            return WrappedOp.new(del_op)
        end
    end #Client

    class WrappedOp < AsyncOp
        def initialize(delegate_op)
            @delegate_op = delegate_op
            @errback = nil
        end

        private
        def wait_value()
            begin  
                @delegate_op.value()
            rescue StandardError > err
                @errback ? @errback.call(err) : raise
            end
        end

        def set_error_handler(errback)
            @errback = errback
        end
    end

end #ZooKeeper
