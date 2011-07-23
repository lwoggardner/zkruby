# coding: utf-8

require 'citrus'
require 'bindata'

class String
   def to_snake_case!
       self.gsub!(/(.)([A-Z])/,'\1_\2')
       self.downcase!
   end
   
   def to_snake_case
       self.clone.to_underscore!
   end
end

module Hoe::Jute
   def initialise_jute
        # Is this something about task dependencies?>
        self.jute_tasks = [:multi, :test, :check_manifest]

        dependency 'citrus', '~> 2.4.0', :development
        dependency 'bindata'
        dependency 'jute'
        
        # TODO load grammar from a here-doc 
        Citrus.load 'jute'
   end

   def define_jute_tasks
         jute_files   = self.spec.files.find_all { |f| f =~ /\.jute$/ }

         record_files = jute_files.map { |f| f.sub(/\.jute$/, ".rb") }

         self.clean_globs += record_files

          rule ".rb" => ".jute" do |t|
              do_jute(t.source,t.name)
          end

          desc "generate jute records" unless jute_files.empty?
          task :compile_jute

          task :compile_jute => parser_files

          jute_tasks.each do |t|
            task t => [:parser, :lexer]
          end 
   end

    JUTE_TO_BINDATA = { :int => "int32be",
                    :long => "int64be",
                    :ustring => "zk_string",
                    :boolean => "zk_boolean",
                    :buffer => "zk_buffer",
                    :float => "float_be",
                    :double => "double_be" }

#TODO - define these as properties
CLIENT_MODULES = { "org.apache.zookeeper.data" => "ZooKeeper::Data",
                   "org.apache.zookeeper.proto" => "ZooKeeper::Proto"}

#TODO - allow these as properties, but merge with well known values
# Some names conflict with ruby - more conflict with JRuby
INVALID_NAMES = { "max" => "maximum", "id" => "identity", "Id" => "Identity", "type" => "_type" }

    def jute_compile(from,to)

        contents = File.read(from)
        modules = Jute.parse(contents).value

        if (selected_modules.size > 0) 
            modules = modules.select() do |k,v|
                selected_modules.has_key?(k)
            end
        end
        
        File.open(to,"w") do |out|
           modules.each_pair do | mod_name, mod_info |
           
           out << " module #{selected_modules[mod_name]}\n"

           mod_info.each_pair do | record_name, record_info |
                record_name = classify(record_name)
                out << "  class #{record_name} < BinData::Record\n"
               
                record_info.each do | field_info |
                    field_name, field_type = field_info
                    field_name = INVALID_NAMES[field_name] if INVALID_NAMES.has_key?(field_name) 
                    field_name.to_snake_case!
                    case field_type
                    when Symbol
                        out << "    #{JUTE_TO_BINDATA[field_type]} :#{field_name}\n"
                    when Array
                        complex_type, *type_args = field_type
                        send(complex_type, field_name, *type_args)
                    else
                        out << field_info.inspect << "\n"
                    end
                end

                out << "  end\n"
           end

           out << " end\n"
        end


end

def vector(name,contained_type)
   case contained_type
   when  Symbol
        puts "    hide :#{name}__length"
        puts "    int32be :#{name}__length, :value => lambda { #{name}.length }"
        puts "    array :#{name}, :type => :#{JUTE_TO_BINDATA[contained_type]}, :initial_length => :#{name}__length"
   when  Array
        complex_type, *type_args = contained_type
        if (complex_type == :record)
           puts "    hide :#{name}__length"
           puts "    int32be :#{name}__length, :value => lambda { #{name}.length }"
           puts "    array :#{name}, :type => :#{type_args[1].downcase}, :initial_length => :#{name}__length"
        else
            #TODO raise typerror
        end
   end

end

# TODO handle modules, for now the record_names must be unique
def record(field_name, module_name, record_name)
    puts "    #{record_name.downcase()} :#{field_name}"
end

# turn a record name into a legitimate ruby class name
def classify(record_name)
   record_name = INVALID_NAMES[record_name] if INVALID_NAMES.has_key?(record_name)
   return record_name.gsub(/(^|_)(.)/) { $2.upcase }
end

