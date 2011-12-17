module ZooKeeper


    class Client

        # @param [String] path path to watch
        # @return [NodeWatcher] 
        def nodes(path,&blk)
            NodeWatcher.new(self,path,&blk)
        end
    end

    # 
    # Asynchronously collects the data for every path under a given node
    # (depth = 1) and watches for further updates
    # 
    # Survives disconnects of the zk client, but not session expiry or any
    #  other unexpected errors
    # 
    #  
    # 
    class NodeWatcher
        include Slf4r::Logger
        attr_reader :zk,:notifier
        # @param [Client] zk an initialized {Client}
        # @param [String] path path to watch
        def initialize(zk,path,notifier = nil, &blk)
            @zk = zk
            @path = path
            @last_children_set = Set.new()
            @notifier =  block_given? ? blk : notifier
            @stopped = false
        end

        # Stop watching
        def stop()
            # all this does is disable the watcher from re-registering itself
            @stopped = true
        end

        def run()
            return if @stopped
            @running = true
            @watch_fired = false
            op = zk.children(@path,self) do |stat,children|
                
              
                errors = @child_errors 
                @child_errors = Set.new()
                errors.each { |p| get_child(p) } if errors

                updates = @child_updates 
                @child_updates = Set.new()
                updates.each { |p| get_child(p) } if errors

                #TODO I wonder how long set difference takes on tens of thousands of entries?
                children_set = Set.new(children)
                removed = @last_children_set - children_set
                added = children_set - @last_children_set
                
                removed.each { |child| receive_data("#{@path}/#{child}",nil,nil) }
                added.each { |child| get_child("#{@path}/#{child}") }

                #because zk guarantees order we can use this
                #to indicate that data for each of the children
                #in the initial set have been retrieved
                op2 = zk.get(@path) do | stat,data |
                    # if there were no errors emit the notification that a pass is complete
                    receive_data(@path,stat,data) if @child_errors.empty?
                   
                    if @child_updates.empty? && @child_errors.empty? && !@watch_fired
                        @running = false
                    else
                        logger.info("run incomplete #{@watch_fired}")
                        run
                    end
                end

                op2.errback do |err|
                    case err
                    when ZK::Error::CONNECTION_LOST
                        #try again
                        run()
                    else
                        unexpected_error(@path,err) 
                    end
                end

                @last_children_set = children_set
            end

            op.errback do |err|
                case err
                when ZK::Error::CONNECTION_LOST
                    run()             
                else
                    unexpected_error(@path,err)
                end
            end
        end
       
        def unexpected_error(path,err)
            return if @stopped
            @stopped = true
            # There is no recovering from these errors so the first unexpected
            # error kills us
            #TODO log the error and notify_error
            notify_error(path,err)
        end

        def notify_error(path,err) 
            receive_data(path,err,nil)
        end

        def receive_data(path,stat,data)
            notifier.call(path,stat,data)
        end

        def get_child(path)
            return if @stopped
            
            op = zk.get(path,self) do |stat,data|
                receive_data(path,stat,data)
            end

            op.errback do |err|
                case err
                when ZK::Error::NO_NODE
                    receive_data(path,nil,nil)
                when ZK::Error::CONNECTION_LOST
                    if @running
                        @child_errors << path
                    else
                        #watch update failed, just try again
                        get_child(path)
                    end
                else 
                    unexpected_error(path,err)
                end
            end
        end

        def process_watch(state,path,event)
            logger.debug {"process watch #{state} #{path} #{event}"}          
            return if @stopped
            case event.to_sym
            when :node_created
                #shouldn't get this because wo don't create exists watches
            when :node_deleted
                #can ignore this because we'll also get a :node_children_changed
            when :node_data_changed
                if @running
                    @child_updates << path
                else 
                    get_child(path)
                end
            when :node_children_changed
                if @running
                    @watch_fired = true
                else
                    run()
                end
            else
                raise ProtocolError , "Unknown event #{event}"
            end
        end
    end

end
