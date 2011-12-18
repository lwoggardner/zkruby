# coding: utf-8
require 'citrus'

class String
    def to_snake_case!
        self.gsub!(/(.)([A-Z])/,'\1_\2')
        self.downcase!
    end

    def to_snake_case
        self.clone.to_underscore!
    end
end

Citrus.load(File.dirname(__FILE__) + "/../jute.citrus")

JUTE_TO_BINDATA = { :int => "int32be",
    :long => "int64be",
    :ustring => "zk_string",
    :boolean => "zk_boolean",
    :buffer => "zk_buffer",
    :float => "float_be",
    :double => "double_be" }

module Jute
class Compiler

    attr_reader :invalid_names

    def initialize()
        @invalid_names = { 
            "max" => "maximum",
            "id" => "identity",
            "Id" => "Identity",
            "type" => "_type"
        }
        @selected_modules = {}
    end

    def compile(input,out,selected_modules={})

        contents = input.read
        modules = Jute.parse(contents).value

        if (selected_modules.size > 0) 
            modules = modules.select() do |k,v|
                selected_modules.has_key?(k)
            end
        end

            modules.each_pair do | mod_name, mod_info |

                out << " module #{selected_modules[mod_name]}\n"

                mod_info.each_pair do | record_name, record_info |
                    record_name = classify(record_name)
                    out << "  class #{record_name} < BinData::Record\n"

                    record_info.each do | field_info |
                        field_name, field_type = field_info
                        field_name = invalid_names[field_name] || field_name 
                        field_name.to_snake_case!
                        case field_type
                        when Symbol
                            out << "    #{JUTE_TO_BINDATA[field_type]} :#{field_name}\n"
                        when Array
                            complex_type, *type_args = field_type
                            send(complex_type, out, field_name, *type_args)
                        else
                            out << field_info.inspect << "\n"
                        end
                    end

                    out << "  end\n"
                end

                out << " end\n"
            end
    end 

    def vector(out, name,contained_type)
        case contained_type
        when  Symbol
            out << "    hide :#{name}__length\n"
            out << "    int32be :#{name}__length, :value => lambda { #{name}.length }\n"
            out << "    array :#{name}, :type => :#{JUTE_TO_BINDATA[contained_type]}, :initial_length => :#{name}__length\n"
        when  Array
            complex_type, *type_args = contained_type
            if (complex_type == :record)
                out << "    hide :#{name}__length\n"
                out << "    int32be :#{name}__length, :value => lambda { #{name}.length }\n"
                out << "    array :#{name}, :type => :#{type_args[1].downcase}, :initial_length => :#{name}__length\n"
            else
                #TODO raise typerror
            end
        end

    end

    # TODO handle modules, for now the record_names must be unique
    def record(out, field_name, module_name, record_name)
        record_name = invalid_names[record_name] || record_name
        out << "    #{record_name.downcase()} :#{field_name}\n"
    end

    # turn a record name into a legitimate ruby class name
    def classify(record_name)
        record_name = invalid_names[record_name] || record_name
        return record_name.gsub(/(^|_)(.)/) { $2.upcase }
    end
end #class Compiler
end #module Jute
if __FILE__ == $0
   compiler = JuteCompiler.new()
   modules = { 
       "org.apache.zookeeper.data" => "ZooKeeper::Data",
       "org.apache.zookeeper.proto" => "ZooKeeper::Proto" }

   compiler.compile(STDIN,STDOUT,modules) 
end
