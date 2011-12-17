require 'spec_helper'

#Helper classes for testing recipes against a mockable zookeeper client
#with pseudo asynchronous behaviour

module ZooKeeper
    # We need to run callback blocks for results
    # in a sequence and inject watches in the middle
    class MockCallback
        attr_accessor :errback, :callback, :err, :results
        def initialize(init_callback)
            @callback = init_callback
        end
        
        def invoke()
            cb,args = err ? [errback,[err]] : [callback,results]
            cb.call(*args)
        end
    end

    # This harness wraps a double on which the calls to a zk client can be stubbed
    # with return values or exceptions (ZooKeeper::Error)
    # If the recipe under test makes an asynchronous call to the mock client, then
    # the callback block is captured alongside the stub result and put in a queue
    # for later processing via #run_queue
    # If the recipe makes a synchronous style call then the existing queue will be run
    # before the stub result is returned directly.
    class MockClient
       attr_reader :double
       def initialize(double)
            @queue = []
            # we delegate all stub methods to our double.
            # if a block is supplied we return an op with the callback and result
            # the op can take an err back. When the tests finish running
            # we invoke the methods in the queue, repeatedly until the queue is empty
            @double = double
       end

       def method_missing(meth,*args,&callback)
            if callback
                cb = ZooKeeper::MockCallback.new(callback)
                begin
                    cb.results = @double.send(meth,*args)
                rescue ZooKeeper::Error => ex
                    cb.err = ex.to_sym
                end
                @queue << cb
                ZooKeeper::AsyncOp.new(cb)
            else
                # we won't get the return from a synchronous call until
                # any pending asynchronous calls have completed
                # only runs the current queued items, any additional activity
                # queued as a result of a callback goes into a new queue
                queued_cbs = @queue
                @queue = []
                queued_cbs.each { |cb| cb.invoke() }
                @double.send(meth,*args) unless callback
            end
       end

       def run_queue()
            until @queue.empty?
                cb = @queue.shift
                cb.invoke()
            end
       end
    end
end #ZooKeeper
