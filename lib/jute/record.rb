module BinData
    class ZKString < ZKBuffer
    end
    class ZKBuffer < BinData::String
    end
    class ZKInt < BinData::Int32be
    end
    class ZKLong < BinData::Int64be

end

module Jute
    # this module gets mixed in to each generated class
    module Record
        module ClassMethods
            def jute_accessor(name,type)
                attr_accessor name
                instance_variable_get("@j_fields") << [name,type]
            end

            def fields
                instance_variable_get("@j_fields")
            end
        end

        # @param out IO|Archive if IO will be wrapped as Archive for format
        # @param tag String
        # @param format Symbol, if supplied then out is expected to be an IO like object
        # The main difference between this serialization and Ruby's native concepts
        # is that we need to know the incoming type in order to deserialize a binary
        # stream (8 bytes for a long etc...)
        def serialize(output,tag=nil,format=:binary)
            unless output.respond_to?(:write_record)
                output = ARCHIVES[format].new(output)
            end
            output.write_record(tag) do
                self.class.fields().each() do |field|
                    name,type = field
                    value = self.send(name)
                    if type.kind_of?(Symbol)
                        output.send("write_#{type}",self.send(name),tag)
                    else
                       #it is a class, just serialize it
                       record = self.send(name)
                       record = type.new() if record.nil?
                       record.serialize(output,tag)
                    end
                end
            end
        end

        def deserialize(input,tag=nil,format=:binary)
           unless input.respond_to?(:read_record)
                input = ARCHIVES[format].new(input)
           end
           input.read_record(tag) do
               self.class.fields().each do |field|
                    name,type = field
                    if type.kind_of?(Symbol)
                        self.send("#{name}=",input.send("read_#{type}",tag))
                    else
                        #type is a class constant
                        record = type.new()
                        record.deserialize(input,tag)
                        self.send("#{name}=",record)
                    end
               end
           end
        end

        def self.included(host_class)
            host_class.extend(ClassMethods)
            host_class.instance_variable_set("@j_fields",[])
        end

    end #module Record

    class LogArchive
        def initialize(io)
            @ba = BinaryArchive.new(io)
        end

        def method_missing(meth,*args,&blk)
            print ("#{meth} #{args}\n")
            @ba.send(meth,*args,&blk)
        end

        def respond_to?(meth)
            [ :write_record, :read_record ].include?  meth
        end
    end

    class BinaryArchive 
        def initialize(io)
            @io = io
        end

        def write_record(tag=nil)
           #no headers/trailers for binary transport
           yield 
        end

        def read_record(tag=nil)
           #no headers/trailers for binary transport
           yield
        end

        def read_boolean(tag=nil)
            @io.read(1).unpack("C")[0] == 0 ? false : true
        end

        def write_boolean(val,tag=nil) 
            @io << [ val ? 0 : 1 ].pack("C")
        end

        def read_int(tag=nil)
            @io.read(4).unpack("N")[0]
        end

        def write_int(val,tag=nil)
            @io << [val.to_i].pack("N")
        end

        def read_long(tag=nil)
            b1,b2 = @io.read(8).unpack("NN")
            b1 << 32 + b2;
        end

        def write_long(val,tag=nil)
            val = val.to_i
            @io << [val >> 32, val & 0xFFFFFFFF].pack("NN")
        end
         
        def read_float(tag=nil)
            @io.read(4).unpack("g")[0]
        end

        def write_float(val,tag=nil)
            @io << [val.to_f].pack("g") 
        end
        
        def read_double(tag=nil)
            @io.read(8).unpack("G")[0]
        end

        def write_double(val,tag=nil)
            @io << [val.to_f].pack("G") 
        end

        def read_string(tag=nil)
            read_buffer().force_encoding("utf-8")
        end

        def write_string(val,tag=nil)
            #TODO val is expected to be a UTF8 string
            write_buffer(val)
        end

        def read_buffer(tag=nil)
            len = read_int()
            return nil if len < 0
            return "" if len == 0
            @io.read(len)
        end

        def write_buffer(val,tag=nil)
            if val.nil?
                write_int(-1)
            else
                @io << [val.length, val].pack("Na*")
            end
        end

    end
    ARCHIVES = { :binary => BinaryArchive, :log => LogArchive } 


        # EM's basic ruby object procotol is the same as what we want to do here
        # read 4 byte packet length, read packet
        # but we might use a StringIO instead of a String.slice!

    #without event machine we just do simple synchronous, blocking, write/read 

end # module Jute

