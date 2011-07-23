module ZooKeeper
   
    class DefaultLogger
        def debug?
            return true
        end
        
        def debug(*args,&blk)
            append("DEBUG:",*args,&blk)
        end

        def info(*args,&blk)
            append("INFO:",*args,&blk)
        end

        def warn(*args,&blk)
            append("WARN:",*args,&blk)
        end

        def error(*args,&blk)
            append("ERROR:",*args,&blk)
        end

        def fatal(*args,&blk)
            append("FATAL:",*args,&blk)
        end

        private 
        def append(*args,&blk)
            args << blk.call.to_s if block_given?
            args << "\n"
            puts args.collect { |a| format_arg(a) }.join(" ")
        end

        def format_arg(arg)
            case arg
            when Exception
              "#{arg.class} #{arg.message}\n\t#{arg.backtrace.join("\n\t")}"
            else
               arg.to_s
            end
        end

    end

    module Logger

        LOGGER = DefaultLogger.new()
        def log
            return LOGGER
        end
    end
end

