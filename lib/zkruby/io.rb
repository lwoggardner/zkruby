require 'set'

# Make EM::Connection behave like IO
# There's lots of buffering going on here
class Strand::EM:IO < EM::Connection
    include IO::Like

    # blocks until one of the supplied EM::Connections is ready
    # or timed out. See IO.select
    # @param [Array<Connection>] read
    # @param [Array<Connection>] write
    # @param [Array<Connection>] error
    # @param [Numeric] timeout maximum time to wait
    def self.select(read,write=[],error=[],timeout=nil)
        # TODO: check our parameters are arrays of IO
        
        # We use a global selector, which is probably not ideal in all circumstances
        # but saves the complexity of each instance having to manage a list of cvs
        # to notify
        cv = ConditionVariable.new() 
      
        begin
           [ read,write,error ].flatten.each { |io| io.selecting(cv) }  

        timeout_at = Time.now + timeout if timeout
        
        # Wait at least one tick
        Strand.pass

        results,timed_out = nil,false
        until results || timed_out

            time_remaining = if timeout then timeout_at - Time.now else nil end 

            r = read.select { |r| r.selected_for_read? }
            w = write.select { |w| w.selected_for_write? }
            e = write.select { |e| e.selected_for_error? }

            results = if r.empty? && w.empty? && e.empty? then nil else [ r, w, e ]
            timed_out = time_remaining && time_remaining <= 0 

            @selector.wait(time_remaining) unless results || timed_out
        end 
        results
        
        ensure
           [ read,write,error ].flatten.each { |io| io.deselecting(cv) }  
                
        end
    end
    
    def selecting(selector_cv)
        @selecting << selector_cv
    end

    def deselecting(selector_cv)
        @selecting.delete(selector_cv)
    end

    def notify
        @selecting.each { |cv| cv.broadcast } 
    end

    def selected_for_read?
        !async_data_buffer.empty? || unbound?
    end

    def selected_for_write?
        connection_completed?
    end

    def selected_for_error?
        # hmmm... there are possibly other reasons for error
        error?
    end

    def initialize()
        super()
        @connected = false
        @unbound = false
        @async_data_buffer = ''
        @async_data_buffer.force_encoding('binary')
        @selecting = Set.new()
        # We don't want IO::Like to do any buffering
        buffer_size=0
        flush_size=0
    end

    def connection_completed()
        @connected = true
        notify
    end

    def receive_data(data)
        # inject directly into io_like internal buffer
        async_receive_buffer << data 
        notify
    end

    def unbind
        @unbound = true
    end

  # @api IO::Like
  # Tell IO::Like we are a duplex stream (unseekable)
  def duplexed?
      true
  end

  private

  attr_reader :async_data_buffer
  attr_reader :waiting

  # @api IO::Like
  # Main read method - somewhat analagous to the low level system read call
  # but we don't have underlying file descriptors to set flags on around the read
  def unbuffered_read(length,*flags)
     
      if !async_data_buffer.empty?
          # ready with data
      elsif unbound?
          raise EOFError
      elsif flags.include?(:nonblock)
        raise Errno::EWOULDBLOCK 
      elsif !read_ready?
        #EOF after blocking
        raise EOFError
      end

      return async_data_buffer.slice!(0,length)
  end

  def unbuffered_write(data,*flags)
    
  end

  def connection_complete?
    @connected || @unbound
  end

  def unbound?
     @unbound
  end

  def read_available?
     !async_data_buffer.empty? 
  end

  # @api IO::Like
  def read_ready?
      IO.select([self]) 
      unbound?
  end

end
