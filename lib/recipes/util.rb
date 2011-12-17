module ZooKeeper

    class UtilErrback
        attr_accessor :errback, :connection_lost
        def invoke(err)
            errback.call(err) if errback
        end
    end

    class Client
        def mkpath(path,acl=ZK::ACL_OPEN_UNSAFE,errback=UtilErrback.new(),&blk)
            return synchronous_call(:mkpath,path,acl) unless block_given?

            connection_lost = false

            path_comp = path.split("/")

            (1-path_comp.length..-1).each do |i|

                sub_path = path_comp[0..i].join("/")
                op = create(sub_path,"",acl) do |path|
                    blk.call if (i == -1)
                end

                op.errback do |err|
                    if i == -1
                        if ZK::Error::NODE_EXISTS === err
                            blk.call()
                        elsif ZK::Error::CONNECTION_LOST === err || ( 
                                                                     ZK::Error::NO_NODE && connection_lost )
                            mkpath(path,acl,errback,&blk)
                        else 
                            errback.invoke(err)
                        end
                    elsif ZK::Error::CONNECTION_LOST === err
                        connection_lost = true
                    end
                end
            end

            AsyncOp.new(errback)

        end

        # async recursive delete
        # if you get a session expiry in the middle of this you will
        # end up with a partially completed recursive delete
        # in all other circumstances it will eventually complete 
        def rmpath(path,version = -1,errback=UtilErrback.new(), &blk)
            return synchronous_call(:rmpath,path,version) unless block_given?


            delete_proc = Proc.new() do 

                del_op = delete(path,version) { blk.call() }

                del_op.errback do |err|
                    case err
                    when ZK::Error::NO_NODE
                        blk.call()
                    when ZK::Error::NOT_EMPTY, ZK::Error::CONNECTION_LOST
                        #try again
                        rmpath(path,version,errback,&blk)
                    else
                        errback.invoke(err)
                    end
                end
            end

            children_op = children(path) do | stat, child_list |

                if child_list.empty?
                    delete_proc.call()
                else
                    error_found = false
                    child_list.each do |child|
                        #note do NOT pass errback in here - we need a new one
                        #to avoid overwriting the top level error callback
                        #and to avoid firing it more than once
                        rm_op = rmpath("#{path}/#{child}",-1) {
                            child_list.delete(child)
                            #when all children have been deleted then we delete ourself
                            delete_proc.call if (child_list.empty?)
                        }

                        #first error will chain back up and since we don't remove the
                        #child, any further successful deletions will be ignored
                        rm_op.errback do |err|
                            errback.invoke(err) unless error_found
                            error_found = true
                        end
                    end
                end

            end

            children_op.errback do |err|
                case err
                when ZK::Error::NO_NODE
                    #someone beat us to it
                    blk.call()
                when ZK::Error::CONNECTION_LOST
                    #try again
                    rmpath(path,version,errback,&blk)
                else
                    errback.invoke(err)
                end
            end
            AsyncOp.new(errback)
        end
    end
end
