require 'stringio'

module ZooKeeper
  # Represents a problem communicating with the ZK cluster
  # client's shouldn't see this
  class ProtocolError < IOError;end


  # Raw object protocol, very similar to EM's native ObjectProtocol
  #
  # Provides
  #   #ready? and #read to be called from the session's read thread
  #   #write to be called from any client thread, or the session's own read thread - in a synchronized manner
  #
  # Expects including class to provide
  #  #send_data
  #  #read_available
  module Protocol

    include Slf4r::Logger

    attr_reader :packet_queue, :packet, :buffer

    def initialize(*args)
      super
      @buffer = Buffer.new()
      @packet = StringIO.new()
      @packet_queue = []
    end

    # reads data from the current packet
    # if none is available raises ProtcolError
    def read(record_class)
      if packet.eof?
        next_packet = packet_queue.shift
        raise ProtocolError, "No available packet" unless next_packet
        packet.string = next_packet
        packet.seek(4,IO::SEEK_SET)
      end
      result = record_class.read(packet)
    end

    def write(*records)
      bin_data = records.collect { |r| r ? r.to_binary_s : nil }
      bin_length = bin_data.reduce(0) { |sum, b| sum += b ? b.length : 0 }
      bin_data.unshift([bin_length].pack("N"))

      send_data(bin_data.join)
      logger.debug { "Sent #{bin_length} byte packet containing  #{bin_data.length} records" }
    end

    # returns true if a new packet is available to consume
    # faises ProtocolError is a previous packet has not been fully consumed
    # returns false if no data is received within the requested timeout
    def ready?(timeout)

      unless packet.eof?
        logger.debug { "Unconsumed record data #{packet.read.unpack("H*")[0]}" }
        raise ProtcolError, "Records not consumed"
      end

      end_time = Time.now + timeout
      until packet_queue.size > 0 || timeout <= 0
        begin
          unless data = read_available(timeout)
            logger.debug { "Read timed out" }
            break;
          end
          buffer << data
          while packet_data = buffer.read_packet
            packet_queue << packet_data
          end
          timeout = end_time - Time.now
        rescue EOFError
          # This is how we expect to end - send a close packet and the
          # server closes the socket
          logger.debug{ "EOF from zookeeper connection" }
          break;
        rescue IOError => ioex
          logger.warn( "IO Error reading from zookeeper connection", ioex )
          break;
        end
      end

      packet_queue.size > 0
    end

    class Buffer
      MIN_PACKET = 5 #TODO Work out what the min packet size really is

      def initialize
        @buffer = String.new().force_encoding('binary')
      end

      def <<(input)
        @buffer << input
        logger.debug { "Received #{input.length} bytes: buffer length = #{@buffer.length}" }
      end

      def read_packet
        return nil unless @buffer.length  > MIN_PACKET

        packet_size = @buffer[0,4].unpack("N").first

        return nil unless @buffer.length >= 4 + packet_size

        logger.debug { "Consumimg packet of size #{packet_size}"}
        return @buffer.slice!(0, 4 + packet_size)
      end
    end
  end
end
